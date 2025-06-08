from pyspark.sql.functions import col, lit, when, struct, explode, collect_list, monotonically_increasing_id
from pyspark.sql.types import StructType, ArrayType, StringType, BooleanType, IntegerType, DoubleType

#handle null value
def handle_null(spark, df):
    def recurse(name, dtype):
        if isinstance(dtype, StructType):
            return struct(*[
                recurse(f"{name}.{f.name}", f.dataType).alias(f.name)
                for f in dtype.fields
            ]).alias(name)
        else:
            if isinstance(dtype, StringType):
                default = lit("0")
            elif isinstance(dtype, BooleanType):
                default = lit(False)
            elif isinstance(dtype, IntegerType):
                default = lit(0)
            elif isinstance(dtype, DoubleType):
                default = lit(0.0)
            else:
                default = lit(None) 

            return when((col(name).isNull()) | (col(name) == ""), default).otherwise(col(name)).alias(name)

    new_columns = []
    for f in df.schema.fields:
        if isinstance(f.dataType, ArrayType):
            new_columns.append(col(f.name))
        else:
            new_columns.append(recurse(f.name, f.dataType))

    return df.select(*new_columns)
