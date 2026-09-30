# merge_dim_order_attributes.py
import psycopg2

MERGE_SQL = """
INSERT INTO dim_order_attributes (payment_method, country, device, source)
SELECT payment_method, country, device, source
FROM staging_dim_order_attributes
ON CONFLICT (payment_method, country, device, source) DO NOTHING;
"""

DROP_STAGING_SQL = "DROP TABLE IF EXISTS staging_dim_order_attributes;"

def run_merge(conn_params):
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(MERGE_SQL)
            inserted = cur.rowcount
            cur.execute(DROP_STAGING_SQL)
        conn.commit()
        print(f"Merge complete — {inserted} new attribute combinations inserted.")
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