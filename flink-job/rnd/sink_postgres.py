from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

t_env.execute_sql("DROP TABLE IF EXISTS kafka_source")
t_env.execute_sql("DROP TABLE IF EXISTS postgres_sink")

# Source Kafka (raw JSON tanpa schema)
t_env.execute_sql("""
CREATE TABLE kafka_source (
    transaction_id STRING,
    customer_id STRING,
    amount INT,
    `timestamp` timestamp(3),
    mercant STRING,
    payment_method STRING,
    location STRING,
    device_id STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions-stream',
    'properties.bootstrap.servers' = 'host.docker.internal:9093',
    'properties.group.id' = 'flink-postgres',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
""")

# Sink ke PostgreSQL
t_env.execute_sql("""
CREATE TABLE postgres_sink (
    transaction_id STRING,
    customer_id STRING,
    amount INT,
    `timestamp` timestamp(3),
    mercant STRING,
    payment_method STRING,
    location STRING,
    device_id STRING,
    PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'public.transactions',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
""")

# Insert streaming data
t_env.execute_sql("""
INSERT INTO postgres_sink
SELECT * FROM kafka_source
""")
