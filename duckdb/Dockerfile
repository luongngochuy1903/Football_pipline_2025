FROM python:3.10

RUN apt-get update && apt-get install -y curl unzip wget

RUN wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip && \
    unzip duckdb_cli-linux-amd64.zip && \
    mv duckdb /usr/local/bin/duckdb && \
    chmod +x /usr/local/bin/duckdb && \
    rm duckdb_cli-linux-amd64.zip
    
RUN pip install --no-cache-dir duckdb pandas pyarrow boto3

WORKDIR /app
COPY . /app

CMD ["tail", "-f", "/dev/null"]