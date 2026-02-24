FROM python:3.11-slim

ARG SPARK_VERSION=3.5.3
ARG HADOOP_PROFILE=hadoop3

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates openjdk-21-jre-headless bash tini \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Install Spark
RUN curl -fL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
  | tar -xz -C /opt \
  && ln -s "/opt/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}" /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash", "-lc", "python -V && spark-submit --version"]