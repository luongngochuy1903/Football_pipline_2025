MariaDB: remote database lưu trữ thông tin bang biểu của hive bằng các table
trino/hive: lưu trữ các schema, database nhưng chỉ là khung, không có dữ lieu, dữ liệu nằm trong minio

CLI: vào vùng query của trino: docker exec -it datalake-trinio trino --server http://localhost:8080 --catalog minio
sql: thêm vào partition cho hive metastore khi trino tạo bang partition mà không hiển thị: CALL system.sync_partition_metadata('test', 'query_parquet', 'ADD');
kiểm tra các partition của trino table trong metastore: SELECT p.*, s.LOCATION FROM DBS d  JOIN TBLS t ON d.DB_ID = t.DB_ID  JOIN PARTITIONS p ON t.TBL_ID = p.TBL_ID  JOIN SDS s ON p.SD_ID = s.SD_ID WHERE t.TBL_NAME = 'query_parquet' AND d.NAME='test';
tạo bảng có partition: create table minio.test.query_parquet( team_name VARCHAR, rank VARCHAR, point VARCHAR, manager VARCHAR, year VARCHAR, month VARCHAR) with (external_location = 's3a://test/teams/', format = 'PARQUET', partitioned_by = ARRAY['year', 'month']);
chạy spark: spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/raw_to_trusted_etl.py
