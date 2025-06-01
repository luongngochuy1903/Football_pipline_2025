import duckdb
from datetime import date, datetime
imoprt boto3

S3_ENDPOINT = 'http://localhost:9000'  # hoặc URL public nếu từ xa
S3_ACCESS_KEY = 'minio_access_key'
S3_SECRET_KEY = 'minio_secret_key'
BUCKET = 'refined'
objects = ["player", "team", "league", "season", "news"]

con = duckdb.connect()

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

con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
con.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
con.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")
con.execute("SET s3_url_style='path';")

# Đọc dữ liệu từ file parquet trên MinIO
for table in objects:
    s3_path = f's3://{BUCKET}/{table}'
df = con.execute(f"SELECT * FROM read_parquet('{s3_path}')").fetchdf()

# Hiển thị kết quả
print(df.head())
