from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode, col, struct, collect_list, when
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

spark = SparkSession.builder \
        .appName("espn transforming") \
        .getOrCreate

def handle_null(spark, df):
    def handle_col(name, dtype):
        if isinstance(dtype, StructType):
            return struct(*[handle_col(f"{name}.{f.name}", f.dataType).alias(f.name) for f in dtype.fields]).alias(name)
        elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
            explode_df = df.withColumn("temp", explode(col(name)))
            explode_handle = handle_null(spark, explode_df.select("temp"))
            return collect_list(explode_handle["temp"]).alias(name)
        else:
            return when(col(name).isNull(), lit("0")).otherwise(col(name)).alias(name)
    
    fields = [handle_col(f.name, f.dataType) for f in df.schema.fields]
    return df.select(*fields)
