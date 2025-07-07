import logging 
import sys 
import traceback 
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath 

# setup logging 
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_velocity_alerts():
    create_sql_velocity_alerts = """
    CREATE TABLE velocity_alerts(
    customer_id string, 
    window_start timestamp(3),
    window_end timestamp(3),
    txn_count bigint, 
    alert_message string 
    )  with (
        'connector' = 'kafka',
        'topic'     = 'velocity_alerts',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json',
        'key.fields' = 'customer_id'
    );
    """
    return create_sql_velocity_alerts


def insert_into_velocity_alerts(table_env):
    logger.info("🔁 Inserting data into sink table...")
    table_env.execute_sql("""
    INSERT INTO velocity_alerts
    select 
    customer_id, 
    window_start,
    window_end,
    count(*) as txn_count, 
    '⚠️ Suspicious velocity: ≥5 transactions in 1 min' AS alert_message
    from table (
            hop(table transactions, 
                descriptor(`timestamp`),
                interval '10' second,
                interval '1' minutes)
    )
    group by customer_id, window_start, window_end   
    having count(*) >=5                                                           
""")
