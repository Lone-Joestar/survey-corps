from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType,DecimalType

spark=SparkSession.builder \
        .appName("load_dim_product") \
        .getOrCreate()

df=spark.read \
    .option("header","true")\
    .csv("/opt/spark-data/products.csv")


df=df.withColumn("product_id",col("product_id").cast(IntegerType()))\
      .withColumn("price_usd",col("price_usd").cast(DecimalType(10,2)))\
      .withColumn("cost_usd",col("cost_usd").cast(DecimalType(10,2)))\
      .withColumn("margin_usd",col("margin_usd").cast(DecimalType(10,2)))

df.write \
    .format("jdbc")\
    .option("url","jdbc:postgresql://host.docker.internal:5433/survey_corps")\
    .option("dbtable","staging_dim_product")\
    .option("user","warehouse")\
    .option("password","warehouse")\
    .option("truncate","true")\
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite")\
    .save()
df.printSchema()