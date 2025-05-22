from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

spark = SparkSession.builder \
        .appName("espn transforming") \
        .getOrCreate

def leagueCleaning(spark, address):
    pass