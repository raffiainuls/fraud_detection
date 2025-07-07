import logging 
import sys 
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath 

# setup logging 
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_env():
    logger.info("Starting Pyflink environment setup......")
    env_setting = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_setting)
    logger.info("TableEnvironment created.")
    return table_env


def create_transactions():
    create_sql_transactions = """
    CREATE TABLE transactions (
        transaction_id STRING,
        customer_id STRING, 
        amount BIGINT, 
        location STRING, 
        merchant STRING,
        payment_method STRING, 
        device_id STRING, 
        `timestamp` TIMESTAMP(3),
        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'transactions-stream',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
    return create_sql_transactions


def create_device_alerts():
    return """
        CREATE TABLE device_alerts (
        device_id STRING,
        window_start TIMESTAMP(3),     -- jika pakai window
        window_end TIMESTAMP(3),       -- jika pakai window
        distinct_customers BIGINT,
        alert_message STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'device_alerts',
            'properties.bootstrap.servers' = 'host.docker.internal:9093',
            'format' = 'json'
        );
    """


def create_table_if_not_exists(table_env, table_name: str, create_sql:str):
    catalog = table_env.get_current_catalog()
    database = table_env.get_current_database()
    obj_path = ObjectPath(database, table_name)

    catalog_obj = table_env.get_catalog(catalog)
    if catalog_obj is not None and not catalog_obj.table_exists(obj_path):
        print(f"Creating table: {table_name}....")
        table_env.execute_sql(create_sql)
        print(f"Table {table_name} created.")
    else:
        print(f"Table {table_name} already exists or catalog not found.")


table_env = create_env()
table_env.execute_sql("DROP TABLE IF EXISTS transactions")
table_env.execute_sql("DROP TABLE IF EXISTS device_alerts")

transactions = create_transactions()
create_table_if_not_exists(table_env, "transactions", transactions)

device_alerts = create_device_alerts()
create_table_if_not_exists(table_env, "device_alerts", device_alerts)



def insert_into_device_alerts(table_env):
    logger.info("📍 Inserting device_alerts anomaly data...")
    table_env.execute_sql("""
   INSERT INTO device_alerts
    SELECT 
        device_id,
        window_start,
        window_end,
        COUNT(DISTINCT customer_id) AS distinct_customers,
        '⚠️ Device anomaly: >3 users on same device in 1 day' AS alert_message
    FROM TABLE(
        HOP(TABLE transactions, DESCRIPTOR(`timestamp`), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
    )
    GROUP BY device_id, window_start, window_end
    HAVING COUNT(DISTINCT customer_id) > 3
        """)

insert_into_device_alerts(table_env)
