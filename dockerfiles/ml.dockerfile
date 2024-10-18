FROM python:3.9-slim

# Install dependencies
RUN apt-get update && apt-get install -y openjdk-17-jdk curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Spark
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3

RUN curl -sL https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -o spark.tgz && \
    tar -xzf spark.tgz -C /opt/ && \
    ln -s /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark && \
    rm spark.tgz

# Install PostgreSQL JDBC Driver
RUN curl -sL https://jdbc.postgresql.org/download/postgresql-42.7.4.jar -o /opt/spark/jars/postgresql-42.7.4.jar

# Environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Create application directories
RUN mkdir -p /app/ml /app/data

# Set working directory
WORKDIR /app/ml

# Copy your application code
COPY code/ml/ml-process.py /app/ml/ml-process.py
COPY dbstatus.py /app/

COPY code/.env /app/ml/.env

# Install Python dependencies
RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas \
    pyspark \
    python-dotenv

# Command to run your application
CMD ["python", "ml-process.py", "--check"]


