import logging 
import sys 
import traceback 
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath 


# setup logging 
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_blacklist_customers():
    create_sql_blacklist_customers = """
    CREATE TABLE blacklist_customers (
    customer_id STRING, 
    description STRING
    ) WITH (
    'connector' = 'kafka',
    'topic' = 'blacklist_customers_list',
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
        'topic' = 'blacklist_devices_list',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """
    return create_sql_blacklist_devices


def create_blacklist():
    create_sql_blacklist = """
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
        description string,
        PRIMARY KEY (transaction_id) NOT ENFORCED
) with (
            'connector' = 'upsert-kafka',
            'topic' = 'blacklist',
            'properties.bootstrap.servers' = 'host.docker.internal:9093',
            'key.format' = 'json',
            'value.format' = 'json'
        )
"""
    return create_sql_blacklist


def insert_into_customers_blacklist(table_env):
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

def insert_into_devices_blacklist(table_env):
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
    INNER JOIN blacklist_devices bc ON t.device_id = bc.device_id
    """)