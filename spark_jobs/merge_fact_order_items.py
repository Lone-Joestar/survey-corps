import psycopg2

MERGE_SQL = """
INSERT INTO fact_order_items (order_id, product_id, unit_price_usd, quantity, line_total_usd)
SELECT s.order_id, s.product_id,
       MAX(s.unit_price_usd) AS unit_price_usd,
       SUM(s.quantity) AS quantity,
       SUM(s.line_total_usd) AS line_total_usd
FROM staging_fact_order_items s
JOIN fact_orders fo ON fo.order_id = s.order_id
JOIN dim_product dp ON dp.product_id = s.product_id
GROUP BY s.order_id, s.product_id
ON CONFLICT (order_id, product_id) DO UPDATE SET
    unit_price_usd = EXCLUDED.unit_price_usd,
    quantity = EXCLUDED.quantity,
    line_total_usd = EXCLUDED.line_total_usd;
"""

DROP_STAGING_SQL = "DROP TABLE IF EXISTS staging_fact_order_items;"

def run_merge(conn_params):
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(MERGE_SQL)
            merged = cur.rowcount
            cur.execute(DROP_STAGING_SQL)
        conn.commit()
        print(f"Merge complete — {merged} order line items upserted.")
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