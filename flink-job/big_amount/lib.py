import logging 
import sys 
import traceback 
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath 

# setup logging 
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_big_amount():
    create_sql_big_amount = """
    create table big_amount (
    transaction_id string,
    customer_id string,
    amount bigint, 
    location string, 
    merchant string, 
    payment_method string, 
    device_id string, 
    `timestamp` timestamp(3),
    PRIMARY KEY (transaction_id) NOT ENFORCED
    ) with (
        'connector' = 'upsert-kafka',
        'topic'     = 'big_amount',
        'properties.bootstrap.servers'  = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """
    return create_sql_big_amount

def insert_into_big_amount(table_env):
    logger.info("📍 Inserting Big Amount anomaly data...")
    table_env.execute_sql("""
    INSERT INTO  big_amount 
    SELECT * FROM transactions 
    where amount > 10000000
    """)

