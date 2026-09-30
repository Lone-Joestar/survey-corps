from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType,BooleanType

if __name__=="__main__":
    spark=SparkSession.builder.appName("load_dim_customer").getOrCreate()

    df=spark.read.option("header","true").csv("/opt/spark-data/customers.csv")

    df = (
        df.withColumn("customer_id", col("customer_id").cast(IntegerType()))
          .withColumn("age", col("age").cast(IntegerType()))
          .withColumn("signup_date", to_date(col("signup_date")))
          .withColumn("marketing_opt_in", col("marketing_opt_in").cast(BooleanType()))
    )

    (
        df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://host.docker.internal:5433/survey_corps")
        .option("dbtable", "staging_dim_customer")
        .option("user", "warehouse")
        .option("password", "warehouse")
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    df.printSchema()