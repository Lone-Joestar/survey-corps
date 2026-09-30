# merge_fact_orders.py
import psycopg2

MERGE_SQL = """
INSERT INTO fact_orders (
    order_id, customer_sk, order_attributes_sk, order_time,
    discount_pct, sub_total_usd, total_usd
)
SELECT s.order_id, dc.customer_sk, doa.order_attributes_sk, s.order_time,
       s.discount_pct, s.sub_total_usd, s.total_usd
FROM staging_fact_orders s
JOIN dim_customer dc
  ON dc.customer_id = s.customer_id
 AND s.order_time >= dc.valid_from
 AND s.order_time < dc.valid_to
JOIN dim_order_attributes doa
  ON doa.payment_method = s.payment_method
 AND doa.country = s.country
 AND doa.device = s.device
 AND doa.source = s.source
ON CONFLICT (order_id) DO UPDATE SET
    customer_sk = EXCLUDED.customer_sk,
    order_attributes_sk = EXCLUDED.order_attributes_sk,
    order_time = EXCLUDED.order_time,
    discount_pct = EXCLUDED.discount_pct,
    sub_total_usd = EXCLUDED.sub_total_usd,
    total_usd = EXCLUDED.total_usd;
"""

DROP_STAGING_SQL = "DROP TABLE IF EXISTS staging_fact_orders;"

def run_merge(conn_params):
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(MERGE_SQL)
            merged = cur.rowcount
            cur.execute(DROP_STAGING_SQL)
        conn.commit()
        print(f"Merge complete — {merged} orders upserted into fact_orders.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    conn_params = {
        "host": "host.docker.internal", "port": 5433, "dbname": "survey_corps",
        "user": "warehouse", "password": "warehouse",
    }
    run_merge(conn_params)