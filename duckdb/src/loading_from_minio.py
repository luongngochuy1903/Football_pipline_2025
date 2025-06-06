import duckdb
from datetime import date, datetime
import boto3

S3_ENDPOINT = 'http://minio:9000' 
S3_ENDPOINT_IP = '172.18.0.7:9000'
S3_ACCESS_KEY = 'minio_access_key'
S3_SECRET_KEY = 'minio_secret_key'
BUCKET = 'refined'
objects = {"player/attacking": "player_attacking_fact", 
           "player/overall": "player_info", 
           "player/defending": "player_defending_fact", 
           "team": "team_info", "league": "leagues", "season": "season", "news": "news",
           "payrolls": "club_expense", "transfer": "club_transfer", 
           "salary":"player_salary"}

con = duckdb.connect("/app/volume/datawarehouse.duckdb")

def get_latest_partition_date(bucket, prefix):
        s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY
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

con.execute("INSTALL 'httpfs';")
con.execute("LOAD 'httpfs';")
con.execute(f"SET s3_endpoint='{S3_ENDPOINT_IP}';")
con.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
con.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")
con.execute("SET s3_url_style='path';")
con.execute("SET s3_use_ssl = false")

# Đọc dữ liệu từ file parquet trên MinIO
for source, table in objects.items():
    year, month, day = get_latest_partition_date(BUCKET, source)
    s3_path = f"s3://{BUCKET}/{source}/year={year}/month={month}/day={day}/*.parquet"
    print(f"Loading data from: {s3_path} → {table}")
    con.execute(f"""INSERT INTO staging.{table} 
                    SELECT * FROM read_parquet('{s3_path}') 
                    """)
    print(f"đã read {s3_path}")

print("hoàn thành")