-- ============================================================
-- Snowflake setup for the ecommerce CDC pipeline
-- 
-- Run as ACCOUNTADMIN once per Snowflake account.
-- Creates: 1 database, 3 schemas, 2 warehouses, 2 custom roles, 1 service user.
--
-- After running, register the connector user's RSA public key:
--   ALTER USER KAFKA_CONNECTOR_USER SET RSA_PUBLIC_KEY='<base64 content>';
-- See README.md "Setup" section for details.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- 1. Database and schemas (medallion-ish layering)
-- pattern: RAW for landed CDC events, STAGING for cleaned, MARTS for analytics.
-- ============================================================
CREATE DATABASE IF NOT EXISTS ECOMMERCE_CDC;

USE DATABASE ECOMMERCE_CDC;
CREATE SCHEMA IF NOT EXISTS RAW;       -- landed CDC events as VARIANT
CREATE SCHEMA IF NOT EXISTS STAGING;   -- dbt staging views (typed, deduped)
CREATE SCHEMA IF NOT EXISTS MARTS;     -- dbt SCD2 dimensions and fact tables

-- ============================================================
-- 2. Dedicated warehouse for CDC ingest
--    Separate from COMPUTE_WH so ingest cost is monitored independently.
--    independently from interactive queries. XSMALL is fine for our volume.
-- ============================================================
CREATE WAREHOUSE IF NOT EXISTS LOAD_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60          -- shut off after 60s idle
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- ============================================================
-- 3. Custom roles (separation of duties / least privilege)
-- ============================================================
CREATE ROLE IF NOT EXISTS CDC_CONNECTOR_ROLE;
CREATE ROLE IF NOT EXISTS DBT_DEVELOPER_ROLE;

-- ============================================================
-- 4. Grants for CDC_CONNECTOR_ROLE 
--    (the streaming sink needs to land events into RAW)
-- ============================================================
GRANT USAGE   ON WAREHOUSE LOAD_WH TO ROLE CDC_CONNECTOR_ROLE;
GRANT OPERATE ON WAREHOUSE LOAD_WH TO ROLE CDC_CONNECTOR_ROLE;

GRANT USAGE ON DATABASE ECOMMERCE_CDC TO ROLE CDC_CONNECTOR_ROLE;
GRANT USAGE ON SCHEMA   ECOMMERCE_CDC.RAW TO ROLE CDC_CONNECTOR_ROLE;

-- The Snowflake Sink Connector auto-creates a table per Kafka topic on first
-- arrival. It needs CREATE TABLE plus INSERT/SELECT on those future tables.
GRANT CREATE TABLE          ON SCHEMA ECOMMERCE_CDC.RAW TO ROLE CDC_CONNECTOR_ROLE;
GRANT INSERT, SELECT ON ALL TABLES    IN SCHEMA ECOMMERCE_CDC.RAW TO ROLE CDC_CONNECTOR_ROLE;
GRANT INSERT, SELECT ON FUTURE TABLES IN SCHEMA ECOMMERCE_CDC.RAW TO ROLE CDC_CONNECTOR_ROLE;

-- Snowpipe Streaming uses internal stages and pipes.
GRANT CREATE STAGE ON SCHEMA ECOMMERCE_CDC.RAW TO ROLE CDC_CONNECTOR_ROLE;
GRANT CREATE PIPE  ON SCHEMA ECOMMERCE_CDC.RAW TO ROLE CDC_CONNECTOR_ROLE;

-- ============================================================
-- 5. Grants for DBT_DEVELOPER_ROLE
--    (read RAW for staging models; full access on STAGING and MARTS)
-- ============================================================
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DBT_DEVELOPER_ROLE;
GRANT USAGE ON DATABASE  ECOMMERCE_CDC TO ROLE DBT_DEVELOPER_ROLE;

-- dbt may need to create new schemas for development environments
GRANT CREATE SCHEMA ON DATABASE ECOMMERCE_CDC TO ROLE DBT_DEVELOPER_ROLE;

GRANT USAGE ON SCHEMA ECOMMERCE_CDC.RAW     TO ROLE DBT_DEVELOPER_ROLE;
GRANT USAGE ON SCHEMA ECOMMERCE_CDC.STAGING TO ROLE DBT_DEVELOPER_ROLE;
GRANT USAGE ON SCHEMA ECOMMERCE_CDC.MARTS   TO ROLE DBT_DEVELOPER_ROLE;

-- Read on RAW (the connector owns the tables, so we need its grant)
USE ROLE CDC_CONNECTOR_ROLE;
GRANT SELECT ON ALL TABLES    IN SCHEMA ECOMMERCE_CDC.RAW TO ROLE DBT_DEVELOPER_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOMMERCE_CDC.RAW TO ROLE DBT_DEVELOPER_ROLE;
USE ROLE ACCOUNTADMIN;

-- Full access on STAGING and MARTS (where dbt builds models)
GRANT ALL ON SCHEMA ECOMMERCE_CDC.STAGING TO ROLE DBT_DEVELOPER_ROLE;
GRANT ALL ON SCHEMA ECOMMERCE_CDC.MARTS   TO ROLE DBT_DEVELOPER_ROLE;

GRANT ALL ON ALL TABLES    IN SCHEMA ECOMMERCE_CDC.STAGING TO ROLE DBT_DEVELOPER_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA ECOMMERCE_CDC.STAGING TO ROLE DBT_DEVELOPER_ROLE;
GRANT ALL ON FUTURE VIEWS  IN SCHEMA ECOMMERCE_CDC.STAGING TO ROLE DBT_DEVELOPER_ROLE;
GRANT ALL ON ALL TABLES    IN SCHEMA ECOMMERCE_CDC.MARTS   TO ROLE DBT_DEVELOPER_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA ECOMMERCE_CDC.MARTS   TO ROLE DBT_DEVELOPER_ROLE;
GRANT ALL ON FUTURE VIEWS  IN SCHEMA ECOMMERCE_CDC.MARTS   TO ROLE DBT_DEVELOPER_ROLE;

-- ============================================================
-- 6. Service user for the Snowflake Sink Connector
--    Authenticates via RSA key-pair (registered after this script runs).
-- ============================================================
CREATE USER IF NOT EXISTS KAFKA_CONNECTOR_USER
    DEFAULT_ROLE      = CDC_CONNECTOR_ROLE
    DEFAULT_WAREHOUSE = LOAD_WH
    DEFAULT_NAMESPACE = ECOMMERCE_CDC.RAW
    COMMENT           = 'Service account for Snowflake Sink connector';

GRANT ROLE CDC_CONNECTOR_ROLE TO USER KAFKA_CONNECTOR_USER;

-- ============================================================
-- 7. Grant DBT_DEVELOPER_ROLE to your personal user 
--    (replace YOUR_USERNAME with the user you log in as)
-- ============================================================
-- GRANT ROLE DBT_DEVELOPER_ROLE TO USER YOUR_USERNAME;

-- ============================================================
-- 8. Verification
-- ============================================================
SHOW ROLES LIKE 'CDC_CONNECTOR_ROLE';
SHOW ROLES LIKE 'DBT_DEVELOPER_ROLE';
SHOW GRANTS TO ROLE CDC_CONNECTOR_ROLE;
SHOW GRANTS TO ROLE DBT_DEVELOPER_ROLE;
SHOW USERS LIKE 'KAFKA_CONNECTOR_USER';
