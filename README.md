Survey Corps

An end-to-end data engineering project: the same e-commerce dataset is processed by a batch pipeline and a streaming pipeline, and the two are continuously checked against each other.

Stack: Spark · PostgreSQL · Airflow · Kafka · Flink · Docker

What it does
Layer	What it builds	Tools
Batch	Star schema in Postgres: SCD Type 2 customer dimension, product dimension, junk dimension, two fact tables	Spark, SQL merges
Orchestration	Daily DAG with retries, parallel-safe load order, backfill support	Airflow
Streaming	Hourly funnel counts and abandoned-cart detection in event time	Kafka, Flink (PyFlink / SQL)
Reconciliation	Hour-by-hour comparison of streaming vs batch order counts, run as an Airflow task that fails on any disagreement	Airflow, SQL
 CSVs ─► Spark ─► staging ─► SQL merge ─► star schema ◄──────────────┐
            ▲                                │                       │ compared
         Airflow DAG ───────────────► reconcile_stream_vs_batch      │ hour by hour
                                                                     │
 events ─► Kafka (3 partitions) ─► Flink (event time) ─► rt_funnel_hourly
                                                      └─► rt_abandoned_carts
Results
760,958 events replayed through Kafka, none lost (partition offsets sum exactly).
Streaming and batch agree: 33,580 purchases in both, with 0 mismatched hours across 24,539 hours compared.
Abandoned carts: Flink found 47,938 sessions (83,963 cart adds); an independent DuckDB calculation over the raw CSV gives the identical numbers.
Idempotent loads: a 3-day Airflow backfill left every table's row count unchanged.
Retries work: tasks killed by out-of-memory recovered on the next attempt without intervention.
Design decisions
Staging overwrite + upsert merge. Every load writes a staging table, then merges into production. Re-running any job, or backfilling, cannot create duplicates.
SCD Type 2 with timestamps, not dates. Date-grained validity ranges collided when a customer changed twice in one day. A partial unique index guarantees one current row per customer, and fact rows join to the dimension version valid at the order's time.
Event time, not processing time. Flink windows use the timestamp carried in each event, with a one-minute watermark allowance. Replaying old data produces the same results as live data would.
Session windows for abandoned carts. A cart is abandoned when a session adds an item, then goes 30 minutes without activity and no purchase. A fixed clock window cannot express "quiet for 30 minutes".
Reconciliation inside the orchestrator. The check runs as a DAG task, so a disagreement between layers shows up as a failed run, not a number someone has to remember to look at.
Problems found and fixed
A daylight-saving bug that matching totals hid. Reconciliation reported 4 mismatched hours although the totals agreed. On 2022-03-13 and 2024-03-10 the local hour 02:00 does not exist; Flink, running in the machine's local timezone, shifted those orders into 03:00. Running the JVM in UTC fixed it, and the check now returns zero mismatches.
Out-of-memory kills. Three Spark jobs starting at once in one container were killed (exit 137). Airflow's retries recovered the run; the DAG now limits concurrent tasks so heavy jobs run one at a time.
Upsert failures on duplicate source rows. Duplicate order lines raised a cardinality violation in the merge; they are now aggregated before insert.
A missing runtime dependency. The Flink Postgres connector needs an OpenLineage library that is not bundled; it is pinned to the version the connector was built against.
Repository layout
dags/           Airflow DAG (daily, 2 retries, reconciliation task)
spark_jobs/     load_* (CSV → staging) and merge_* (staging → production)
streaming/      Kafka compose, event replay, Flink jobs
spark/ warehouse/ docker-compose.yaml   Spark, Postgres, Airflow
sql/schema.sql  schema snapshot generated with pg_dump
Running it

Docker needs at least 6 GB of memory.

bash
cd warehouse && docker compose up -d        # Postgres on :5433
cd spark && docker compose up -d            # Spark master/worker
docker compose up -d                        # Airflow (repo root)
cd streaming && docker compose up -d        # Kafka

# streaming (Python 3.11, Java 11/17)
uv venv --python 3.11 && source .venv/bin/activate
uv pip install confluent-kafka "apache-flink==2.2.*"
python replay_events.py && python funnel_job.py
python abandoned_cart_job.py

Connector JARs must match the Flink version: Kafka 5.0.0-2.2, JDBC 4.1.0-2.2, postgresql-42.7.4, openlineage-java-1.32.0. Trigger the DAG survey_corps_warehouse_load from Airflow to load the warehouse.

Limitations
Flink jobs are started manually; Airflow orchestrates the batch load and reads the streaming output for reconciliation.
Events come from a replayed file, not a live source.
Abandoned-cart logic uses session windows; keyed state with timers and MATCH_RECOGNIZE are not implemented.
Credentials are local development defaults.
