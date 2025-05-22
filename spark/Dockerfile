FROM bitnami/spark:3.5.4
USER root
RUN apt-get update && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN install_packages curl
USER 1001
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar --output /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar