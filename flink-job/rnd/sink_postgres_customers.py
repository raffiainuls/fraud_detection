from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

t_env.execute_sql("DROP TABLE IF EXISTS kafka_source")
t_env.execute_sql("DROP TABLE IF EXISTS postgres_sink")

# Source Kafka (raw JSON tanpa schema)
t_env.execute_sql("""
CREATE TABLE kafka_source (
    customer_id STRING,
    description STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'blacklist_customers',
    'properties.bootstrap.servers' = 'host.docker.internal:9093',
    'properties.group.id' = 'flink-postgres3',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
""")

# Sink ke PostgreSQL
t_env.execute_sql("""
CREATE TABLE postgres_sink (
    customer_id STRING,
    description STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'public.blacklist_customers_list',
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

