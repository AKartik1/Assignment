from pyspark.sql import SparkSession

def init_spark(app_name: str):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "mysql-connector-j-8.0.33.jar") \
        .getOrCreate()
    return spark

def close_spark(spark):
    spark.stop()