FROM python:3.10

RUN apt-get update && apt-get install -y curl unzip wget

RUN pip install --no-cache-dir duckdb pandas pyarrow boto3

WORKDIR /app
COPY . /app

CMD ["tail", "-f", "/dev/null"]