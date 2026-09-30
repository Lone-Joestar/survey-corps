from airflow.sdk import dag,task
from datetime import datetime,timedelta
import docker

SPARK_CONTAINER="survey-corps-spark-master"


def run_in_spark_container(command: str) -> None:
       client=docker.from_env()
       container=client.containers.get(SPARK_CONTAINER)
       exit_code,output=container.exec_run(cmd=command)
       log=output.decode("utf-8",errors="replace")
       print(log)
       if exit_code !=0:
              raise RuntimeError(f"Command failed with exit code {exit_code}:{command}\n{log}")
default_args={
       "retries":2,
       "retry_delay":timedelta(minutes=2)
}








@dag(
    dag_id="survey_corps_warehouse_load",
    schedule="@daily",
    start_date=datetime(2026,1,1),
    catchup=False,
    tags=["survey-corps"],
    default_args=default_args
)

def warehouse_load():

    @task
    def load_dim_customer():
        run_in_spark_container("/opt/spark/bin/spark-submit /opt/spark/work-dir/load_dim_customer.py")
        run_in_spark_container("python3 /opt/spark/work-dir/scd2_merge_customer.py")

    @task
    def load_dim_product():
        run_in_spark_container("/opt/spark/bin/spark-submit /opt/spark/work-dir/spark_write_staging.py")
        run_in_spark_container("python3 /opt/spark/work-dir/merge_staging_to_prod.py")

    @task
    def load_dim_order_attributes():
        run_in_spark_container("/opt/spark/bin/spark-submit /opt/spark/work-dir/load_dim_order_attributes.py")
        run_in_spark_container("python3 /opt/spark/work-dir/merge_dim_order_attributes.py")

    @task
    def load_fact_orders():
        run_in_spark_container("/opt/spark/bin/spark-submit /opt/spark/work-dir/load_fact_orders.py")
        run_in_spark_container("python3 /opt/spark/work-dir/merge_fact_orders.py")

    @task
    def load_fact_order_items():
        run_in_spark_container("/opt/spark/bin/spark-submit /opt/spark/work-dir/load_fact_order_items.py")
        run_in_spark_container("python3 /opt/spark/work-dir/merge_fact_order_items.py")

    @task
    def reconcile_stream_vs_batch():
        import psycopg2
        conn = psycopg2.connect(host="host.docker.internal", port=5433,
                                dbname="survey_corps", user="warehouse", password="warehouse")
        with conn, conn.cursor() as cur:
            cur.execute("""
                WITH stream AS (SELECT window_start hr, events sp
                                FROM rt_funnel_hourly WHERE event_type = 'purchase'),
                    batch  AS (SELECT date_trunc('hour', order_time) hr, count(*) bo
                                FROM fact_orders GROUP BY 1)
                SELECT count(*) FILTER (WHERE sp IS DISTINCT FROM bo),
                    coalesce(sum(sp), 0), coalesce(sum(bo), 0)
                FROM stream s FULL JOIN batch b USING (hr)
            """)
            mismatched, stream_total, batch_total = cur.fetchone()
        conn.close()
        print(f"mismatched_hours={mismatched} stream={stream_total} batch={batch_total}")
        if mismatched or stream_total != batch_total:
            raise ValueError(f"Stream/batch mismatch: {mismatched} hours, "
                            f"stream={stream_total}, batch={batch_total}")
        
    c= load_dim_customer()
    p=load_dim_product()
    a=load_dim_order_attributes()

    fo=load_fact_orders()
    foi=load_fact_order_items()

    [c,a] >>fo
    [p,fo] >> foi
    foi>> reconcile_stream_vs_batch()
warehouse_load()

