from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType,DecimalType

def write_staging(df,jdbc_url,staging_table,db_props):
    """
    Overwrite a staging model with the current batch 
    Staging is always full overwrite
    """
    (
        df.write 
        .format("jdbc")
        .option("url",jdbc_url)
        .option("dbtable",staging_table)
        .option("user",db_props["user"])
        .option("password",db_props["password"])
        .option("driver","org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
if __name__=="__main__":
    spark=SparkSession.builder.appName("load_dim_product").getOrCreate()

    df=spark.read.option("header","true").csv("/opt/spark-data/products.csv")

    df=(
        df.withColumn("product_id", col("product_id").cast(IntegerType()))
          .withColumn("price_usd", col("price_usd").cast(DecimalType(10, 2)))
          .withColumn("cost_usd", col("cost_usd").cast(DecimalType(10, 2)))
          .withColumn("margin_usd", col("margin_usd").cast(DecimalType(10, 2)))
    )
    write_staging(
        df,
        jdbc_url="jdbc:postgresql://host.docker.internal:5433/survey_corps",
        staging_table="staging_dim_product",
        db_props={"user": "warehouse", "password": "warehouse"},
    )
    df.printSchema()