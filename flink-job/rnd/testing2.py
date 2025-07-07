import logging 
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath

# setup logging 
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_env():
    logger.info("Starting PyFlink environment setup...")
    env_setting = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_setting)
    logger.info("TableEnvironment created.")
    return table_env

def create_transactions():
    create_sql_transactions = """
    CREATE TABLE transactions2 (
        transaction_id STRING,
        customer_id STRING,
        amount BIGINT,
        merchant STRING,
        location STRING,
        payment_method STRING,
        device_id STRING,
        `timestamp` STRING,
        event_time AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd HH:mm:ss'),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'transactions',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    );
    """
    return create_sql_transactions

def create_table_if_not_exists(table_env, table_name: str, create_sql: str):
    catalog = table_env.get_current_catalog()
    database = table_env.get_current_database()
    obj_path = ObjectPath(database, table_name)

    # LANGSUNG .table_exists()
    catalog_obj = table_env.get_catalog(catalog)
    if catalog_obj is not None and not catalog_obj.table_exists(obj_path):
        print(f"Creating table: {table_name}...")
        table_env.execute_sql(create_sql)
        print(f"Table {table_name} created.")
    else:
        print(f"Table {table_name} already exists or catalog not found.")


# Init Table Env & Create Source Table
table_env = create_env()
transactions_sql = create_transactions()
create_table_if_not_exists(table_env, "transactions2", transactions_sql)

# Create Sink Table for Velocity Fraud
table_env.execute_sql("""
CREATE TABLE velocity_fraud2 (
    customer_id STRING,
    tx_count BIGINT,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    PRIMARY KEY (customer_id, window_start) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'velocity_fraud',
    'properties.bootstrap.servers' = 'host.docker.internal:9093',
    'key.format' = 'json',
    'value.format' = 'json'
);
""")

# Insert into velocity_fraud table
velocity_fraud_query = """
INSERT INTO velocity_fraud2
SELECT
  customer_id,
  COUNT(*) AS tx_count,
  window_start,
  window_end
FROM TABLE(
  HOP(TABLE transactions2, DESCRIPTOR(event_time), INTERVAL '10' SECOND, INTERVAL '1' MINUTE)
)
GROUP BY customer_id, window_start, window_end
HAVING COUNT(*) >= 5
"""

table_env.execute_sql(velocity_fraud_query)
