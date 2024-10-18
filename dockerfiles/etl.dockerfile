FROM python:3.9-slim

# Install Java and necessary tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Spark
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3

RUN curl -sL "https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" -o spark.tgz && \
    tar -xzf spark.tgz -C /opt/ && \
    ln -s "/opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION" /opt/spark && \
    rm spark.tgz

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH="$PATH:$SPARK_HOME/bin"
ENV PYSPARK_PYTHON=python
ENV PYSPARK_DRIVER_PYTHON=python
ENV SPARK_LOCAL_IP=127.0.0.1
ENV PYSPARK_SUBMIT_ARGS="--conf spark.driver.memory=2g --conf spark.executor.memory=2g pyspark-shell"

# Install Python dependencies
RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas \
    python-dotenv \
    pyspark

# Create application directories
RUN mkdir -p /app/etl /app/data
RUN mkdir -p /app/code/libs


# Set the working directory
WORKDIR /app

# Copy application files
COPY code/etl/etl_process.py ./etl_process.py
COPY code/.env ./
COPY dbstatus.py ./
COPY data/ /app/data/

# Copy the JDBC driver
COPY code/libs/postgresql-42.7.4.jar /app/code/libs/

# Command to run the application
CMD ["python", "./etl_process.py"]
