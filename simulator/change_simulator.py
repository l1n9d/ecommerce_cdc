"""
Change simulator for the e-commerce CDC pipeline.

Continuously generates realistic operational changes against the RDS Postgres
database. Designed to produce a rich stream of CDC events that exercise the
SCD2 dimensional models we'll build downstream.

DESIGN CHOICES 
--------------------------------------
1. Single-process simulator with weighted-random event selection.
   Production would split this into per-domain services with their own rate
   limits; single-process is simpler for a portfolio demo and easier to control
   centrally.

2. Poisson-style random delays between events (not fixed tick).
   Real systems have bursty variance; this looks more realistic in the demo
   than a metronome.

3. Full order lifecycle simulation (insert order, decrement inventory,
   progress status over time). Generates the realistic cascade of related
   changes that makes the CDC pipeline interesting to watch end-to-end.

4. Bias toward changing many distinct entities rather than hitting the same
   ones repeatedly -- because SCD2 demos care about entities-with-history,
   not raw event count.

5. Each operation is its own transaction (autocommit-style). Real applications
   bundle related writes; we keep them separate so each WAL entry shows
   cleanly as one CDC event in Debezium.

USAGE
-----
    python simulator/change_simulator.py --rate 5 --duration 1800

    --rate: average events per second (default 2)
    --duration: total runtime in seconds (default unlimited; Ctrl+C to stop)
"""
import argparse
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg2
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

fake = Faker('en_US')

DB_CONFIG = {
    'host': os.environ['PG_HOST'],
    'port': int(os.environ.get('PG_PORT', 5432)),
    'database': os.environ.get('PG_DATABASE', 'ecommerce'),
    'user': os.environ.get('PG_USER', 'postgres'),
    'password': os.environ['PG_PASSWORD'],
}

# Event weights -- tuned to produce rich SCD2 stories without overwhelming the
# database. Bias customer/product updates higher than realistic real-world rates
# because the pipeline demo benefits from frequent dim history.
EVENT_WEIGHTS = {
    'new_order':            45,   # high volume, drives most CDC
    'progress_order_status': 20,  # confirmed -> shipped -> delivered
    'restock_inventory':     8,   # warehouse activity
    'new_customer':          10,
    'update_customer_addr':  10,  # SCD2 candy
    'update_customer_phone': 3,
    'update_product_price':  3,   # SCD2 candy
    'cancel_order':          1,   # rare but realistic
}

PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'gift_card']
CARRIERS = ['UPS', 'FedEx', 'USPS', 'DHL']

# Track stats for end-of-run summary
event_counts = {k: 0 for k in EVENT_WEIGHTS}
running = True

def handle_sigint(signum, frame):
    global running
    print("\n[shutting down gracefully]")
    running = False

signal.signal(signal.SIGINT, handle_sigint)


def connect():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True   # each event = its own transaction = one WAL entry
    return conn


# ---------------------------------------------------------------------------
# Event handlers -- each function performs one operation and returns success
# ---------------------------------------------------------------------------

def event_new_order(cur):
    cur.execute("SELECT customer_id, street_address, city, state, zip_code FROM customers ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return False
    customer_id, addr, city, state, zip_code = row

    cur.execute("""
        SELECT product_id, current_price FROM products
        WHERE is_active = TRUE AND product_id IN (
            SELECT product_id FROM inventory WHERE quantity_on_hand > 0
        )
        ORDER BY random() LIMIT %s
    """, (random.randint(1, 4),))
    items = cur.fetchall()
    if not items:
        return False

    # 85% ship to customer's address, 15% to a different address
    if random.random() < 0.85:
        ship = (addr, city, state, zip_code)
    else:
        ship = (fake.street_address(), fake.city(), fake.state_abbr(), fake.zipcode())

    order_total = Decimal('0')
    line_items = []
    for product_id, current_price in items:
        qty = random.randint(1, 3)
        unit_price = Decimal(str(current_price))  # use current price; price drift is at price-update time
        line_total = unit_price * qty
        order_total += line_total
        line_items.append((product_id, qty, unit_price, line_total))

    cur.execute("""
        INSERT INTO orders (customer_id, order_status, order_total, payment_method,
                            shipping_address, shipping_city, shipping_state, shipping_zip)
        VALUES (%s, 'pending', %s, %s, %s, %s, %s, %s)
        RETURNING order_id
    """, (customer_id, order_total, random.choice(PAYMENT_METHODS), *ship))
    order_id = cur.fetchone()[0]

    for product_id, qty, unit_price, line_total in line_items:
        cur.execute("""
            INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total)
            VALUES (%s, %s, %s, %s, %s)
        """, (order_id, product_id, qty, unit_price, line_total))
        # Decrement inventory -- this generates a separate CDC event on inventory
        cur.execute("""
            UPDATE inventory
            SET quantity_on_hand = GREATEST(0, quantity_on_hand - %s),
                updated_at = NOW()
            WHERE product_id = %s
        """, (qty, product_id))
    return True


def event_progress_order_status(cur):
    """Move an order forward: pending -> confirmed -> shipped -> delivered."""
    cur.execute("""
        SELECT order_id, order_status FROM orders
        WHERE order_status IN ('pending', 'confirmed', 'shipped')
        ORDER BY random() LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        return False
    order_id, status = row

    next_status = {'pending': 'confirmed', 'confirmed': 'shipped', 'shipped': 'delivered'}[status]

    cur.execute("UPDATE orders SET order_status = %s, updated_at = NOW() WHERE order_id = %s",
                (next_status, order_id))

    # Side effects on shipments table
    if next_status == 'shipped':
        cur.execute("""
            INSERT INTO shipments (order_id, carrier, tracking_number, shipped_at, status)
            VALUES (%s, %s, %s, NOW(), 'in_transit')
        """, (order_id, random.choice(CARRIERS), f"TRK{random.randint(100000000, 999999999)}"))
    elif next_status == 'delivered':
        cur.execute("""
            UPDATE shipments SET delivered_at = NOW(), status = 'delivered', updated_at = NOW()
            WHERE order_id = %s
        """, (order_id,))
    return True


def event_restock_inventory(cur):
    cur.execute("""
        SELECT product_id, reorder_threshold FROM inventory
        WHERE quantity_on_hand < reorder_threshold * 2
        ORDER BY random() LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        return False
    product_id, threshold = row
    cur.execute("""
        UPDATE inventory
        SET quantity_on_hand = quantity_on_hand + %s, updated_at = NOW()
        WHERE product_id = %s
    """, (random.randint(50, 200), product_id))
    return True


def event_new_customer(cur):
    first = fake.first_name()
    last = fake.last_name()
    email = f"{first.lower()}.{last.lower()}{random.randint(1, 99999)}@{fake.free_email_domain()}"
    cur.execute("""
        INSERT INTO customers (email, first_name, last_name, phone,
                               street_address, city, state, zip_code, country)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'US')
    """, (email, first, last, fake.phone_number()[:20],
          fake.street_address(), fake.city(), fake.state_abbr(), fake.zipcode()))
    return True


def event_update_customer_addr(cur):
    """The headline SCD2 demo event: a customer moves."""
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return False
    customer_id = row[0]
    cur.execute("""
        UPDATE customers
        SET street_address = %s, city = %s, state = %s, zip_code = %s, updated_at = NOW()
        WHERE customer_id = %s
    """, (fake.street_address(), fake.city(), fake.state_abbr(), fake.zipcode(), customer_id))
    return True


def event_update_customer_phone(cur):
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return False
    cur.execute("UPDATE customers SET phone = %s, updated_at = NOW() WHERE customer_id = %s",
                (fake.phone_number()[:20], row[0]))
    return True


def event_update_product_price(cur):
    """SCD2 candy: price change. Bias toward small adjustments, occasional bigger swings."""
    cur.execute("SELECT product_id, current_price FROM products WHERE is_active = TRUE ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return False
    product_id, current_price = row
    # 80% small change (-10% to +10%), 20% bigger swing
    if random.random() < 0.8:
        multiplier = random.uniform(0.90, 1.10)
    else:
        multiplier = random.uniform(0.70, 1.30)
    new_price = (Decimal(str(current_price)) * Decimal(str(multiplier))).quantize(Decimal('0.01'))
    if new_price < Decimal('0.99'):
        new_price = Decimal('0.99')
    cur.execute("UPDATE products SET current_price = %s, updated_at = NOW() WHERE product_id = %s",
                (new_price, product_id))
    return True


def event_cancel_order(cur):
    cur.execute("""
        SELECT order_id FROM orders
        WHERE order_status IN ('pending', 'confirmed') ORDER BY random() LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        return False
    cur.execute("UPDATE orders SET order_status = 'cancelled', updated_at = NOW() WHERE order_id = %s",
                (row[0],))
    return True


HANDLERS = {
    'new_order':             event_new_order,
    'progress_order_status': event_progress_order_status,
    'restock_inventory':     event_restock_inventory,
    'new_customer':          event_new_customer,
    'update_customer_addr':  event_update_customer_addr,
    'update_customer_phone': event_update_customer_phone,
    'update_product_price':  event_update_product_price,
    'cancel_order':          event_cancel_order,
}


def pick_event():
    events, weights = zip(*EVENT_WEIGHTS.items())
    return random.choices(events, weights=weights)[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rate', type=float, default=2.0,
                        help='average events per second (default 2)')
    parser.add_argument('--duration', type=int, default=0,
                        help='total runtime seconds (0 = run until Ctrl+C)')
    args = parser.parse_args()

    avg_delay = 1.0 / args.rate
    print(f"Starting simulator: ~{args.rate} events/sec, "
          f"{'unlimited' if args.duration == 0 else f'{args.duration}s'} runtime")
    print("Ctrl+C to stop early\n")

    conn = connect()
    started_at = time.time()
    last_summary = started_at
    total = 0
    failed = 0

    try:
        with conn.cursor() as cur:
            while running:
                if args.duration and (time.time() - started_at) > args.duration:
                    break
                event = pick_event()
                try:
                    ok = HANDLERS[event](cur)
                    if ok:
                        event_counts[event] += 1
                        total += 1
                    else:
                        failed += 1
                except Exception as e:
                    print(f"[error] {event}: {e}")
                    failed += 1
                # Poisson-style: exponential delay between events
                time.sleep(random.expovariate(1.0 / avg_delay))

                # Periodic summary every 30s
                if time.time() - last_summary > 30:
                    elapsed = int(time.time() - started_at)
                    rate = total / max(elapsed, 1)
                    print(f"[{elapsed}s] {total} events ({rate:.1f}/s), {failed} skipped/errored")
                    last_summary = time.time()
    finally:
        conn.close()
        elapsed = int(time.time() - started_at)
        print(f"\n=== Simulator stopped after {elapsed}s ===")
        print(f"Total events: {total}")
        print(f"Skipped/errored: {failed}")
        print("\nBreakdown:")
        for evt, count in sorted(event_counts.items(), key=lambda x: -x[1]):
            print(f"  {evt:25s} {count}")


if __name__ == '__main__':
    main()
