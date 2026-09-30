Survey Corps

A data-engineering portfolio project: one e-commerce dataset, processed two ways and checked against itself.

Batch layer: Spark loads CSVs into a Postgres star schema (SCD Type 2 customer dimension), orchestrated by Airflow.
Streaming layer: events are replayed into Kafka; Flink computes hourly funnel counts and abandoned-cart sessions in event time and writes them to Postgres.
Reconciliation: an Airflow task compares stream and batch hour by hour and fails the run if they disagree.
                       ┌────────────── batch ──────────────┐
 CSVs ──► Spark ──► staging tables ──► merge (SQL) ──► star schema (Postgres :5433)
            ▲                                                    │
         Airflow DAG  survey_corps_warehouse_load                │ compared hour by hour
            │                                                    ▼
            └──────────────► reconcile_stream_vs_batch ◄── rt_funnel_hourly
                                                                 ▲
 events.csv ──► replay_events.py ──► Kafka (3 partitions) ──► Flink ──┤
                                                                      └─► rt_abandoned_carts
Data model
Table	Type	Notes
dim_customer	SCD Type 2	valid_from / valid_to TIMESTAMPs, is_current, partial unique index on current rows
dim_product	Type 1	
dim_order_attributes	Junk dimension	combines low-cardinality order flags
fact_orders	Fact	joins dim_customer point-in-time on order_time
fact_order_items	Fact	duplicate lines aggregated before insert
rt_funnel_hourly	Streaming output	events and distinct sessions per hour per event type
rt_abandoned_carts	Streaming output	sessions with add_to_cart and no purchase

Loads are idempotent: each job overwrites a staging table, then a SQL merge upserts into production.

Repository layout
dags/warehouse_load.py      Airflow DAG (daily, 2 retries, 2 min delay)
spark_jobs/                 load_* (CSV -> staging) and merge_* (staging -> production)
spark/                      Spark master/worker compose
warehouse/                  Postgres warehouse compose
streaming/                  Kafka compose, replay script, Flink jobs, connector JARs
sql/schema.sql              schema snapshot generated with pg_dump
docker-compose.yaml         Airflow
Running it

Everything runs locally in Docker. Give Docker at least 6 GB of memory; Spark jobs get killed (exit 137) below that.

Warehouse: cd warehouse && docker compose up -d (Postgres on localhost:5433, db survey_corps, dev-only credentials warehouse / warehouse).
Spark: cd spark && docker compose up -d. Copy spark_jobs/* to /opt/spark/work-dir in survey-corps-spark-master, and put the Postgres JDBC jar in /opt/spark/jars.
Airflow: docker compose up -d from the repo root, then trigger survey_corps_warehouse_load.
Streaming:
bash
   cd streaming && docker compose up -d
   docker exec survey-corps-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 \
     --create --topic events --partitions 3 --replication-factor 1
   uv venv --python 3.11 && source .venv/bin/activate
   uv pip install confluent-kafka "apache-flink==2.2.*" duckdb
   python replay_events.py            # 760,958 events
   python funnel_job.py               # hourly funnel -> rt_funnel_hourly
   python abandoned_cart_job.py       # session windows -> rt_abandoned_carts

Flink 2.2 needs Java 11 or 17, and the connector JARs in streaming/jars/ must match the Flink version (Kafka 5.0.0-2.2, JDBC 4.1.0-2.2, postgresql-42.7.4, openlineage-java-1.32.0).

Verified results
Check	Result
Row counts after full load	20,001 / 1,197 / 1,126 / 33,580 / 59,053 (customer, product, order attrs, orders, order items)
Kafka ingest	760,958 events, 3 partitions, none lost
Stream vs batch purchases	33,580 = 33,580, 0 mismatched hours (24,539 hours compared)
Abandoned carts (Flink) vs independent DuckDB calculation	47,938 sessions / 83,963 cart adds, exact match
Backfill of 3 days (Sep 25 to 27)	all tables unchanged, loads are idempotent
Retries	tasks killed by out-of-memory succeeded on attempt 2 without intervention
Problems found along the way
Out-of-memory kills. Three Spark jobs started at once inside one container and were killed (exit 137). Airflow's retries recovered the run, and the DAG now sets max_active_tasks=1 so heavy jobs run one at a time.
Daylight-saving bug caught by reconciliation. Totals matched, but four hours did not. On 2022-03-13 and 2024-03-10 the local hour 02:00 does not exist, and Flink running in the machine's local timezone shifted those orders into 03:00. Running the JVM in UTC (TZ=UTC, set at the top of the job files) fixed it. A totals-only check would never have found it.
SCD2 validity collisions. Date-grained validity ranges collided on same-day changes, so valid_from / valid_to are TIMESTAMPs.
Connector dependency chain. The JDBC Postgres connector needs openlineage-java at runtime, which is not bundled.
Limitations
The Flink jobs are started by hand; Airflow orchestrates only the batch load and the reconciliation read.
Events come from a replayed file, not a live source.
Abandoned carts use session windows. Keyed state with timers and MATCH_RECOGNIZE were not implemented.
Credentials are local development defaults.
