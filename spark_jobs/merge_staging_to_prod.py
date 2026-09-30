import psycopg2

MERGE_SQL = """
INSERT INTO dim_product (
    product_id, category, name, price_usd, cost_usd, margin_usd
)
SELECT product_id, category, name, price_usd, cost_usd, margin_usd
FROM staging_dim_product
ON CONFLICT (product_id) DO UPDATE SET
    category   = EXCLUDED.category,
    name       = EXCLUDED.name,
    price_usd  = EXCLUDED.price_usd,
    cost_usd   = EXCLUDED.cost_usd,
    margin_usd = EXCLUDED.margin_usd;
"""

DROP_STAGING_SQL = "DROP TABLE IF EXISTS staging_dim_product;"

def run_merge(conn_params):
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(MERGE_SQL)
            merged = cur.rowcount
            cur.execute(DROP_STAGING_SQL)
        conn.commit()
        print(f"Merge complete — {merged} rows upserted into dim_product, staging dropped.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    conn_params = {
        "host": "host.docker.internal",
        "port": 5433,
        "dbname": "survey_corps",
        "user": "warehouse",
        "password": "warehouse",
    }
    run_merge(conn_params)