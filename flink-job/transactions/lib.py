import logging 
import sys 
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath 

# setup logging 
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

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


