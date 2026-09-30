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
    'properties.group.id' = 'abandoned-cart-job',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
""")

t_env.execute_sql("""
CREATE TABLE abandoned_sink (
    session_id   BIGINT,
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    cart_adds    BIGINT,
    total_events BIGINT,
    PRIMARY KEY (session_id, window_start) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5433/survey_corps',
    'table-name' = 'rt_abandoned_carts',
    'username' = 'warehouse',
    'password' = 'warehouse'
)
""")

# One window per session: it closes when the session has been silent for 30 minutes
# (in event time). Keep only sessions that added to cart but never purchased.
t_env.execute_sql("""
INSERT INTO abandoned_sink
SELECT session_id, window_start, window_end,
       SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END),
       COUNT(*)
FROM TABLE(
    SESSION(TABLE events PARTITION BY session_id, DESCRIPTOR(event_time), INTERVAL '30' MINUTE))
GROUP BY session_id, window_start, window_end
HAVING SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) > 0
   AND SUM(CASE WHEN event_type = 'purchase'    THEN 1 ELSE 0 END) = 0
""").wait()
claude