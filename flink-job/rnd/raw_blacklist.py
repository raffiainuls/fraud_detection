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
        `timestamp` TIMESTAMP(3)
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


def create_blacklist_customers():
    create_sql_blacklist_customers = """
    CREATE TABLE blacklist_customers (
        customer_id STRING,
        description STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'blacklist_customers',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
    return create_sql_blacklist_customers

def create_blacklist_devices():
    create_sql_blacklist_devices = """
    CREATE TABLE blacklist_devices (
        device_id STRING,
        description STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'blacklist_devices',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
    return create_sql_blacklist_devices



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
table_env.execute_sql("DROP TABLE IF EXISTS blacklist")
table_env.execute_sql("DROP TABLE IF EXISTS blacklist_devices")
table_env.execute_sql("DROP TABLE IF EXISTS blacklist_customers")

transactions = create_transactions()
create_table_if_not_exists(table_env, "transactions", transactions)

blacklist_customers = create_blacklist_customers()
create_table_if_not_exists(table_env, "blacklist_customers", blacklist_customers)

blacklist_devices = create_blacklist_devices()
create_table_if_not_exists(table_env, "blacklist_devices", blacklist_devices)



table_env.execute_sql("""
    create table blacklist (
        transaction_id string, 
        customer_id string, 
        amount bigint, 
        location string, 
        merchant string,
        payment_method string, 
        device_id string, 
        `timestamp` timestamp(3),
        label string,
        description string
) with (
            'connector' = 'kafka',
            'topic' = 'blacklist',
            'properties.bootstrap.servers' = 'host.docker.internal:9093',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
""")

def insert_into_blacklist(table_env):
    logger.info("🔁 Inserting data into sink table...")

    # 1. Insert dari customer blacklist
    table_env.execute_sql("""
    INSERT INTO blacklist
    SELECT 
        t.transaction_id, 
        t.customer_id,
        amount, 
        location,
        merchant,
        payment_method,
        t.device_id,
        `timestamp`,
        'blacklist customer' AS label,
        bc.description
    FROM transactions t
    INNER JOIN blacklist_customers bc ON t.customer_id = bc.customer_id
    """)


insert_into_blacklist(table_env)
