from pyspark.sql.functions import col, lit, when, struct, explode, collect_list, monotonically_increasing_id
from pyspark.sql.types import StructType, ArrayType

def handle_null(spark, df):
    def recurse(name, dtype):
        if isinstance(dtype, StructType):
            return struct(*[
                recurse(f"{name}.{f.name}", f.dataType).alias(f.name)
                for f in dtype.fields
            ]).alias(name)
        else:
            return when(col(name).isNull(), lit("0")).otherwise(col(name)).alias(name)

    new_columns = []
    for f in df.schema.fields:
        if isinstance(f.dataType, ArrayType):
            # giữ nguyên array (không xử lý null bên trong)
            new_columns.append(col(f.name))
        else:
            new_columns.append(recurse(f.name, f.dataType))

    return df.select(*new_columns)
