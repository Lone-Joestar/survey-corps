# load_fact_orders.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import IntegerType, DecimalType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("load_fact_orders").getOrCreate()

    df = spark.read.option("header", "true").csv("/opt/spark-data/orders.csv")

    df = (
        df.withColumn("order_id", col("order_id").cast(IntegerType()))
          .withColumn("customer_id", col("customer_id").cast(IntegerType()))
          .withColumn("order_time", to_timestamp(col("order_time")))
          .withColumn("discount_pct", col("discount_pct").cast(DecimalType(10, 2)))
          .withColumn("sub_total_usd", col("subtotal_usd").cast(DecimalType(10, 2)))
          .withColumn("total_usd", col("total_usd").cast(DecimalType(10, 2)))
          .select("order_id", "customer_id", "order_time", "payment_method",
                  "discount_pct", "sub_total_usd", "total_usd",
                  "country", "device", "source")
    )

    (
        df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://host.docker.internal:5433/survey_corps")
        .option("dbtable", "staging_fact_orders")
        .option("user", "warehouse")
        .option("password", "warehouse")
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    df.printSchema()