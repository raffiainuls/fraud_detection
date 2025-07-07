import logging 
import sys 
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath 

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_geolocation_alerts():
    return """
    CREATE TABLE geolocation_alerts (
        customer_id STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        distinct_locations BIGINT,
        alert_message STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'geolocation_alerts',
        'properties.bootstrap.servers' = 'host.docker.internal:9093',
        'key.format' = 'json',
        'value.format' = 'json',
        'key.fields' = 'customer_id'
    )
    """

def insert_into_geolocation_alerts(table_env):
    logger.info("📍 Inserting geolocation anomaly data...")
    table_env.execute_sql(
        """
    INSERT INTO geolocation_alerts
    select 
        customer_id,
        window_start,
        window_end,
        count(distinct location) as distinct_locations,
        '🚨 Geolocation anomaly: ≥2 locations in <5 minutes' AS alert_message
    from table(
        hop(table transactions, 
            descriptor(`timestamp`), 
            interval '1' minute, 
            interval '5' minute)
    )
    group by customer_id, window_start, window_end
    having count(distinct location) >= 2
""")