# load_dim_order_attributes.py
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("load_dim_order_attributes").getOrCreate()

    df = spark.read.option("header", "true").csv("/opt/spark-data/orders.csv")

    attrs = df.select("payment_method", "country", "device", "source").distinct()

    (
        attrs.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://host.docker.internal:5433/survey_corps")
        .option("dbtable", "staging_dim_order_attributes")
        .option("user", "warehouse")
        .option("password", "warehouse")
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    attrs.printSchema()