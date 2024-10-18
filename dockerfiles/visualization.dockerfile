FROM python:3.9-slim

# Install dependencies
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl nginx && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Spark
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3

RUN curl -sL https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -o spark.tgz && \
    tar -xzf spark.tgz -C /opt/ && \
    ln -s /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark && \
    rm spark.tgz

# Environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Create application directories
RUN mkdir -p /app/htdocs /app/code

# Set working directory
WORKDIR /app/code

# Copy application code and configuration
COPY code/visualization/visualization-process.py /app/code/visualization-process.py
COPY code/visualization/nginx_config/nginx.conf /etc/nginx/nginx.conf
COPY code/ /app/code/
COPY code/.env ./
COPY dbstatus.py /app/
COPY code/libs/postgresql-42.7.4.jar /opt/spark/jars/


# Install Python dependencies
RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas \
    pyspark \
    python-dotenv \
    bokeh

# Expose Nginx's port
EXPOSE 80

# Start Nginx and run the visualization script
CMD ["sh", "-c", "nginx && python visualization-process.py"]
