import logging 
import sys 
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath 

# setup logging 
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)



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
            'key.format' = 'json',
            'value.format' = 'json',
            'key.fields' = 'device_id'
        )
    """

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