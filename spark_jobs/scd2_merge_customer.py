import psycopg2

CLOSE_CHANGED_SQL = """
UPDATE dim_customer d
SET valid_to = CURRENT_TIMESTAMP,
    is_current = FALSE
FROM staging_dim_customer s
WHERE d.customer_id = s.customer_id
  AND d.is_current = TRUE
  AND (d.name, d.email, d.country, d.age, d.marketing_opt_in)
      IS DISTINCT FROM
      (s.name, s.email, s.country, s.age, s.marketing_opt_in);
"""

INSERT_NEW_VERSIONS_SQL = """
INSERT INTO dim_customer (
    customer_id, name, email, country, age, signup_date,
    marketing_opt_in, valid_from, valid_to, is_current
)
SELECT s.customer_id, s.name, s.email, s.country, s.age, s.signup_date,
       s.marketing_opt_in, CURRENT_TIMESTAMP, TIMESTAMP '9999-12-31', TRUE
FROM staging_dim_customer s
LEFT JOIN dim_customer d
       ON d.customer_id = s.customer_id AND d.is_current = TRUE
WHERE d.customer_id IS NULL;
"""

DROP_STAGING_SQL = "DROP TABLE IF EXISTS staging_dim_customer;"

def run_scd2_merge(conn_params):
    conn = psycopg2.connect(**conn_params)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(CLOSE_CHANGED_SQL)
            closed = cur.rowcount
            cur.execute(INSERT_NEW_VERSIONS_SQL)
            inserted = cur.rowcount
            cur.execute(DROP_STAGING_SQL)
        conn.commit()
        print(f"SCD2 merge complete — {closed} versions closed, {inserted} new versions inserted.")
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
    run_scd2_merge(conn_params)