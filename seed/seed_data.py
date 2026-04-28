"""
Seed initial e-commerce data into RDS Postgres.

Generates realistic-looking data with Faker:
  ~1000 customers
  ~200 products
  ~5000 orders with 1-5 line items each
  Shipments for ~80% of confirmed orders

Idempotent-ish: TRUNCATEs before inserting so re-running gives a clean state.
For real production seed scripts you'd handle this differently, but for a
portfolio project the simplicity is fine.
"""
import os
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

fake = Faker('en_US')
Faker.seed(42)
random.seed(42)

# Connection details from environment - never hardcode these
DB_CONFIG = {
    'host': os.environ['PG_HOST'],
    'port': int(os.environ.get('PG_PORT', 5432)),
    'database': os.environ.get('PG_DATABASE', 'ecommerce'),
    'user': os.environ.get('PG_USER', 'postgres'),
    'password': os.environ['PG_PASSWORD'],
}

NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 200
NUM_ORDERS = 5000

PRODUCT_CATEGORIES = [
    'Electronics', 'Home & Kitchen', 'Books', 'Clothing',
    'Sports & Outdoors', 'Beauty', 'Toys & Games', 'Grocery',
]

ORDER_STATUSES = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled']
ORDER_STATUS_WEIGHTS = [5, 10, 15, 65, 5]  # most orders end up delivered

PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'gift_card']

CARRIERS = ['UPS', 'FedEx', 'USPS', 'DHL']

def truncate_all(cur):
    """Wipe everything before re-seeding. CASCADE handles FK relationships."""
    print("Truncating existing data...")
    cur.execute("""
        TRUNCATE customers, products, inventory, orders, order_items, shipments
        RESTART IDENTITY CASCADE;
    """)

def seed_customers(cur):
    print(f"Seeding {NUM_CUSTOMERS} customers...")
    rows = []
    for _ in range(NUM_CUSTOMERS):
        first = fake.first_name()
        last = fake.last_name()
        # Email based on name + random number to avoid Faker collisions on unique constraint
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 99999)}@{fake.free_email_domain()}"
        # Stagger created_at across last 2 years for realistic distribution
        created = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 730))
        rows.append((
            email, first, last,
            fake.phone_number()[:20],
            fake.street_address(),
            fake.city(),
            fake.state_abbr(),
            fake.zipcode(),
            'US',
            created, created,
        ))
    execute_values(cur, """
        INSERT INTO customers (
            email, first_name, last_name, phone,
            street_address, city, state, zip_code, country,
            created_at, updated_at
        ) VALUES %s
    """, rows)

def seed_products(cur):
    print(f"Seeding {NUM_PRODUCTS} products...")
    rows = []
    for i in range(NUM_PRODUCTS):
        category = random.choice(PRODUCT_CATEGORIES)
        # Different categories have different price ranges - looks more real
        if category == 'Electronics':
            price = round(random.uniform(20, 1500), 2)
        elif category == 'Books':
            price = round(random.uniform(5, 50), 2)
        elif category == 'Grocery':
            price = round(random.uniform(2, 30), 2)
        else:
            price = round(random.uniform(10, 200), 2)

        rows.append((
            f"SKU-{i+1:06d}",
            fake.catch_phrase()[:255],
            category,
            fake.text(max_nb_chars=200),
            price,
            round(random.uniform(0.1, 10.0), 3),
            random.random() > 0.05,  # 5% inactive products
        ))
    execute_values(cur, """
        INSERT INTO products (
            sku, name, category, description, current_price, weight_kg, is_active
        ) VALUES %s
    """, rows)

def seed_inventory(cur):
    print("Seeding inventory for all products...")
    cur.execute("""
        INSERT INTO inventory (product_id, quantity_on_hand, reorder_threshold)
        SELECT product_id,
               (random() * 500)::int,
               (random() * 20 + 5)::int
        FROM products;
    """)

def seed_orders_and_items(cur):
    print(f"Seeding {NUM_ORDERS} orders with line items...")

    cur.execute("SELECT customer_id FROM customers")
    customer_ids = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT product_id, current_price FROM products WHERE is_active = TRUE")
    products = cur.fetchall()

    order_rows = []
    item_rows = []

    for _ in range(NUM_ORDERS):
        customer_id = random.choice(customer_ids)
        # Shipping address often same as customer's, sometimes different
        if random.random() < 0.85:
            cur.execute("""
                SELECT street_address, city, state, zip_code FROM customers WHERE customer_id = %s
            """, (customer_id,))
            ship = cur.fetchone()
        else:
            ship = (fake.street_address(), fake.city(), fake.state_abbr(), fake.zipcode())

        status = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS)[0]
        placed_at = datetime.now(timezone.utc) - timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        # Choose 1-5 unique products for this order
        line_items = random.sample(products, k=random.randint(1, 5))
        order_total = Decimal('0')
        items_for_this_order = []
        for product_id, current_price in line_items:
            qty = random.randint(1, 4)
            # Tiny price variance to simulate price drift since the order was placed
            unit_price = (Decimal(str(current_price)) * Decimal(str(random.uniform(0.95, 1.05)))).quantize(Decimal('0.01'))
            line_total = unit_price * qty
            order_total += line_total
            items_for_this_order.append((product_id, qty, unit_price, line_total))

        order_rows.append((
            customer_id, status, order_total,
            random.choice(PAYMENT_METHODS),
            ship[0], ship[1], ship[2], ship[3],
            placed_at, placed_at,
        ))
        # We don't have order_ids yet, so stash items keyed by index for now
        item_rows.append(items_for_this_order)

    # Insert orders, get back generated IDs
    inserted_ids = execute_values(cur, """
        INSERT INTO orders (
            customer_id, order_status, order_total, payment_method,
            shipping_address, shipping_city, shipping_state, shipping_zip,
            placed_at, updated_at
        ) VALUES %s
        RETURNING order_id
    """, order_rows, fetch=True)

    # Now flatten items list with the actual order_ids
    flat_items = []
    for (order_id_tuple, items) in zip(inserted_ids, item_rows):
        order_id = order_id_tuple[0]
        for product_id, qty, unit_price, line_total in items:
            flat_items.append((order_id, product_id, qty, unit_price, line_total))

    print(f"Inserting {len(flat_items)} order_items...")
    execute_values(cur, """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total)
        VALUES %s
    """, flat_items)

    return inserted_ids

def seed_shipments(cur):
    print("Seeding shipments for orders that progressed past 'pending'...")
    cur.execute("""
        SELECT order_id, order_status, placed_at
        FROM orders
        WHERE order_status IN ('shipped', 'delivered', 'confirmed')
    """)
    candidate_orders = cur.fetchall()

    rows = []
    for order_id, status, placed_at in candidate_orders:
        # 80% of these have a shipment record
        if random.random() > 0.20:
            shipped_at = placed_at + timedelta(days=random.randint(1, 3))
            if status == 'delivered':
                delivered_at = shipped_at + timedelta(days=random.randint(2, 7))
                ship_status = 'delivered'
            elif status == 'shipped':
                delivered_at = None
                ship_status = 'in_transit'
            else:  # confirmed but not yet shipped
                shipped_at = None
                delivered_at = None
                ship_status = 'pending'

            rows.append((
                order_id,
                random.choice(CARRIERS),
                f"TRK{random.randint(100000000, 999999999)}",
                shipped_at,
                delivered_at,
                ship_status,
            ))

    if rows:
        execute_values(cur, """
            INSERT INTO shipments (order_id, carrier, tracking_number, shipped_at, delivered_at, status)
            VALUES %s
        """, rows)
    print(f"Inserted {len(rows)} shipments.")

def report_counts(cur):
    print("\n=== Final row counts ===")
    for tbl in ['customers', 'products', 'inventory', 'orders', 'order_items', 'shipments']:
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        n = cur.fetchone()[0]
        print(f"  {tbl}: {n}")

def main():
    print("Connecting to RDS...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            truncate_all(cur)
            seed_customers(cur)
            seed_products(cur)
            seed_inventory(cur)
            seed_orders_and_items(cur)
            seed_shipments(cur)
            report_counts(cur)
        conn.commit()
        print("\nSeed complete and committed.")
    except Exception:
        conn.rollback()
        print("\nSeed FAILED, transaction rolled back.")
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    main()
