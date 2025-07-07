import logging 
import sys 
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath 


# setup logging 
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_env():
    logger.info("Strating Pyflink environment setup....")
    env_setting = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_setting)
    logger.info("TableEnvironment created.")
    return table_env


def create_transactions():
    create_sql_transactions = """
    create table transactions(
    transaction_id bigint, 
    customer_id string, 
    amount bigint, 
    location string, 
    payment_method string, 
    device_id string, 
    `timestamp` TIMESTAMP(3)
    ) with (
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'host.docker.internal:9093',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
    )

    """
    return create_sql_transactions


def create_table_if_not_exists(table_env, table_name:str, create_sql: str):
    catalog = table_env.get_current_catalog()
    database = table_env.get_current_database()
    obj_path = ObjectPath(database, table_name)

    catalog_inst = table_env.get_catalog(catalog)

    if not catalog_inst.table_exists(obj_path):
        logger.info("creating table transactions......")
        table_env.execute_sql(create_sql)
        logger.info("tablle transactions created.")
    else:
        logger.info("table already exist")

table_env = create_env()
transactions = create_transactions()
create_table_if_not_exists(table_env, "transactions", transactions)


table_env.execute_sql("""
CREATE TABLE kafka (
    transaction_id bigint,
    customer_id string,
    amount bigint,
    location STRING,
    payment_method STRING,
    device_id STRING,
    `timestamp` TIMESTAMP(3)
) WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'transaction_testing',
            'properties.bootstrap.servers' = 'host.docker.internal:9093',
            'key.format' = 'json',
            'value.format' = 'json'
        )
)
""")

table_env.execute_sql("""
INSERT INTO print_sink
SELECT * FROM transactions
""")

