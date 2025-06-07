from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from datetime import date, datetime
import boto3
from pyspark.sql.functions import col, to_json, struct, md5
import logging

def checking_duplicated(spark, target, raw_df, address, list_value):
    latest = get_latest_partition_date(target, address)
    if not latest:
        return False
    year, month, day = latest
    trusted_df = spark.read.json(f"s3a://{target}/{address}/year={year}/month={month}/day={day}")
    def hash_column(df, columns):
        return df.select(md5(to_json(struct(*[col(f"`{c}`") for c in columns]))).alias("hash")) \
             .rdd.map(lambda row: row["hash"]).collect()

    raw_list = hash_column(raw_df, list_value)
    trusted_list = hash_column(trusted_df, list_value)

    print("Raw Hashes là:", raw_list)
    print("Trusted Hashes là:", trusted_list)
    for item in raw_list:
        if item not in trusted_list:
            return False
    return True
    

def get_latest_partition_date(bucket, prefix):
        s3_client = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minio_access_key",
            aws_secret_access_key="minio_secret_key"
        )
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
                print(f"There is no data available in Trusted/{prefix}, ready to load")
                return None
        contents = response.get("Contents", [])
        partitions = set()

        for obj in contents:
            key = obj["Key"]
            parts = key.split("/")
            try:
                year = next(int(p.split("=")[1]) for p in parts if p.startswith("year="))
                month = next(int(p.split("=")[1]) for p in parts if p.startswith("month="))
                day = next(int(p.split("=")[1]) for p in parts if p.startswith("day="))
                partitions.add(datetime(year, month, day))
                print(f"Ngày tháng năm partition gần nhất là: {year}/{month}/{day}")
            except:
                continue 

        if not partitions:
            return None
        latest = max(partitions)
        return latest.year, latest.month, latest.day

def get_updated_news(spark, target, raw_df, address):
    latest = get_latest_partition_date(target, address)
    if not latest:
        return raw_df
    year, month, day = latest
    trusted_df = spark.read.json(f"s3a://{target}/{address}/year={year}/month={month}/day={day}")
    trustedcheck_df = trusted_df.withColumn("hashing_df", md5(col("url")))
    rawcheck_df = raw_df.withColumn("hashing_df", md5(col("url")))
    exist_hashes = set(row["hashing_df"] for row in trustedcheck_df.select(col("hashing_df")).collect())
    df_new = rawcheck_df.filter(~col("hashing_df").isin(exist_hashes))
    return df_new