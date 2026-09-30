# Survey Corps

One e-commerce dataset, processed two ways — a **batch** pipeline and a **streaming** pipeline — and checked against each other every run.

**Spark · Postgres · Airflow · Kafka · Flink · Docker**

```
 CSVs ─► Spark ─► Postgres star schema ◄──── reconcile ────┐
            ▲                                              │
         Airflow                                           │
                                                           │
 events ─► Kafka ─► Flink ─► hourly funnel + abandoned carts
```

## Highlights

- **Batch:** star schema with an SCD Type 2 customer dimension; every load is idempotent (staging overwrite, then upsert).
- **Orchestration:** Airflow DAG with retries. A 3-day backfill left every table unchanged.
- **Streaming:** 760,958 events through Kafka; Flink computes hourly funnel counts and abandoned-cart sessions in event time.
- **Reconciliation:** an Airflow task compares streaming and batch order counts hour by hour and fails the run on any difference. Result: 33,580 = 33,580, zero mismatched hours.
- **Verified independently:** Flink's abandoned-cart count (47,938 sessions) matches a separate DuckDB calculation exactly.

## The bug worth telling

Stream and batch totals matched, but reconciliation reported 4 mismatched hours. All four fell on the spring daylight-saving changes (2022-03-13, 2024-03-10), when the local hour 02:00 doesn't exist. Flink, running in the machine's local timezone, pushed those orders into 03:00. Running it in UTC fixed the mismatch. A check on totals alone would never have caught it.

## Design choices

- **SCD2 uses timestamps, not dates** — date ranges collided when a customer changed twice in one day.
- **Event time, not processing time** — replayed history gives the same answers as live data.
- **Session windows for abandoned carts** — "no activity for 30 minutes" can't be expressed with fixed clock windows.

## Run it

Needs Docker (6 GB+ memory), Java 11/17 and Python 3.11.

```bash
cd warehouse && docker compose up -d      # Postgres
cd spark && docker compose up -d          # Spark
docker compose up -d                      # Airflow
cd streaming && docker compose up -d      # Kafka

cd streaming
uv venv --python 3.11 && source .venv/bin/activate
uv pip install confluent-kafka "apache-flink==2.2.*"
python replay_events.py
python funnel_job.py
python abandoned_cart_job.py
```

Then trigger the `survey_corps_warehouse_load` DAG in Airflow. Flink needs matching connector JARs in `streaming/jars/` (Kafka `5.0.0-2.2`, JDBC `4.1.0-2.2`, `postgresql-42.7.4`, `openlineage-java-1.32.0`).

## Layout

```
dags/        Airflow DAG
spark_jobs/  load and merge jobs
streaming/   Kafka, event replay, Flink jobs
sql/         schema snapshot
```

## Limitations

Flink jobs are started by hand. Events are replayed from a file rather than a live source. Keyed state with timers and `MATCH_RECOGNIZE` are not implemented. Credentials are local development defaults.
