import os
os.environ["TZ"]="UTC"
from pyflink.table import EnvironmentSettings, TableEnvironment

JARS = [
    "flink-sql-connector-kafka-5.0.0-2.2.jar",
    "flink-connector-jdbc-core-4.1.0-2.2.jar",
    "flink-connector-jdbc-postgres-4.1.0-2.2.jar",
    "postgresql-42.7.4.jar",
    "openlineage-java-1.32.0.jar",
]
t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
jar_dir = os.path.abspath("jars")
cfg = t_env.get_config()
cfg.set("pipeline.jars", ";".join(f"file://{jar_dir}/{j}" for j in JARS))
cfg.set("parallelism.default", "3")
cfg.set("table.exec.source.idle-timeout", "10 s")
cfg.set("table.local-time-zone", "UTC")


# Source: the Kafka topic, with event time taken from the JSON "timestamp" field
t_env.execute_sql("""
CREATE TABLE events (
    event_id      BIGINT,
    session_id    BIGINT,
    `timestamp`   STRING,
    event_type    STRING,
    product_id    INT,
    qty           INT,
    cart_size     INT,
    payment       STRING,
    discount_pct  DOUBLE,
    amount_usd    DOUBLE,
    event_time AS TO_TIMESTAMP(REPLACE(`timestamp`, 'T', ' ')),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'events',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'funnel-job',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
""")

# Sink: Postgres table. PRIMARY KEY makes the connector upsert instead of append.
t_env.execute_sql("""
CREATE TABLE funnel_sink (
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    event_type   STRING,
    events       BIGINT,
    sessions     BIGINT,
    PRIMARY KEY (window_start, event_type) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5433/survey_corps',
    'table-name' = 'rt_funnel_hourly',
    'username' = 'warehouse',
    'password' = 'warehouse'
)
""")

# The job: hourly tumbling windows on event time, written to Postgres
t_env.execute_sql("""
INSERT INTO funnel_sink
SELECT window_start, window_end, event_type,
       COUNT(*), COUNT(DISTINCT session_id)
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '1' HOUR))
GROUP BY window_start, window_end, event_type
""").wait()