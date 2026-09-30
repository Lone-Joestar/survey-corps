from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DecimalType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("load_fact_order_items").getOrCreate()

    df = spark.read.option("header", "true").csv("/opt/spark-data/order_items.csv")

    df = (
        df.withColumn("order_id", col("order_id").cast(IntegerType()))
          .withColumn("product_id", col("product_id").cast(IntegerType()))
          .withColumn("unit_price_usd", col("unit_price_usd").cast(DecimalType(10, 2)))
          .withColumn("quantity", col("quantity").cast(IntegerType()))
          .withColumn("line_total_usd", col("line_total_usd").cast(DecimalType(10, 2)))
    )

    (
        df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://host.docker.internal:5433/survey_corps")
        .option("dbtable", "staging_fact_order_items")
        .option("user", "warehouse")
        .option("password", "warehouse")
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    df.printSchema()