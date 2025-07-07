from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

t_env.execute_sql("DROP TABLE IF EXISTS kafka_source")
t_env.execute_sql("DROP TABLE IF EXISTS postgres_sink")

# Source Kafka (raw JSON tanpa schema)
t_env.execute_sql("""
CREATE TABLE kafka_source (
    device_id STRING,
    description STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'blacklist_devices',
    'properties.bootstrap.servers' = 'host.docker.internal:9093',
    'properties.group.id' = 'flink-postgres4',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
""")

# Sink ke PostgreSQL
t_env.execute_sql("""
CREATE TABLE postgres_sink (
    device_id STRING,
    description STRING,
    PRIMARY KEY (device_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'public.blacklist_device_list',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
""")

# Insert streaming data
t_env.execute_sql("""
INSERT INTO postgres_sink
SELECT  DISTINCT * FROM kafka_source
""")

