from datetime import datetime
import time
import os
import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import functions as F
from dotenv import load_dotenv
import sys
sys.path.append('/app')
import dbstatus

# Load environment variables from .env file
load_dotenv()

# Accessing PostgreSQL environment variables
db_name = os.getenv('POSTGRES_DB')
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initializing Spark session
spark = SparkSession.builder \
    .appName('Data_Engineering_Project') \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .getOrCreate()


# Set Spark logging level
spark.sparkContext.setLogLevel("INFO")

def get_last_quarter():
    """Retrieve the last quarter from the status table."""
    try:
        connection = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
        with connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT status FROM status ORDER BY timestamp DESC LIMIT 1;")
            last_quarter = cursor.fetchone()
            logger.info(f"Retrieved last quarter: {last_quarter[0] if last_quarter else 'None'}")
            return last_quarter[0] if last_quarter else None
    except Exception as e:
        logger.error(f"Error retrieving last quarter: {e}")
        return None
    finally:
        connection.close()

def create_table():
    """Create the required table for the ML process in the PostgreSQL database."""
    try:
        connection = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
        with connection.cursor() as mycursor:
            current_quarter = (datetime.now().month - 1) // 3 + 1
            last_quarter = get_last_quarter()

            if current_quarter != last_quarter:
                logger.info("Dropping existing table due to new quarter...")
                mycursor.execute('DROP TABLE IF EXISTS "Cat Bill Amount";')

            # Create table
            sql_create = """
                CREATE TABLE IF NOT EXISTS "Cat Bill Amount" (
                    "Medical Condition" VARCHAR(60),
                    "Hospital" VARCHAR(60),
                    "Insurance Provider" VARCHAR(60),
                    "Quarter" INT,
                    "Avg Bill Amount" DECIMAL,
                    "Bill Cat" VARCHAR(20)
                );
            """
            logger.info("Executing SQL to create tables...")
            mycursor.execute(sql_create)
            logger.info("Table created successfully.")

            # Clear the table for fresh data
            mycursor.execute("DELETE FROM \"Cat Bill Amount\";")
            logger.info("Cleared Cat Bill Amount table.")

            connection.commit()
            logger.info("Changes committed to the database.")

    except Exception as e:
        logger.error(f"Error occurred while creating table: {e}")
        raise
    finally:
        connection.close()
        logger.info("Database connection closed.")


def extract():
    """Extract data from the admission table."""
    connection = None
    mycursor = None
    try:
        # Wait for ETL process to finish
        while dbstatus.checkStatus(2):
            logger.info("Waiting for ETL to finish...")
            time.sleep(60)

        logger.info("Logging process status: ML 1/2 - Process started")
        dbstatus.logStatus(3, "ML 1/2 - Process started")

        jdbc_url = f"jdbc:postgresql://{db_host}/{db_name}"
        properties = {
            "user": db_user,
            "password": db_password,
            "driver": "org.postgresql.Driver"
        }

        logger.info("Reading data from the admission table...")
        data = spark.read.jdbc(url=jdbc_url, table="public.admission", properties=properties)

        logger.info("Displaying the first 10 rows of the admission table:")
        data.show(10)

        logger.info("Checking schema of the admission table...")
        data.printSchema()

        # Filter out rows with NULL values in relevant columns
        filtered_data = data.filter(
            data["Medical Condition"].isNotNull() &
            data["Hospital"].isNotNull() &
            data["Average Bill Amount"].isNotNull() &
            data["Insurance Provider"].isNotNull() &
            data["Quarter"].isNotNull()
        )

        filtered_row_count = filtered_data.count()
        logger.info(f"Number of rows after filtering: {filtered_row_count}")

        # Aggregate data using DataFrame API
        aggregated_data = filtered_data.groupBy("Medical Condition", "Hospital", "Insurance Provider", "Quarter") \
            .agg(F.round(F.avg("Average Bill Amount"), 2).alias("Avg_Bill_Amount")) \
            .orderBy("Avg_Bill_Amount", ascending=False)

        logger.info("Aggregated data using DataFrame API:")
        aggregated_data.show()

        return aggregated_data

    except Exception as e:
        logger.error(f"Error during data extraction: {e}")
        raise

def create_model(data):
    """Run KMeans clustering on the data."""
    logger.info("Starting KMeans clustering...")

    # Index the Hospital and Insurance Provider columns
    hospital_indexer = StringIndexer(inputCol="Hospital", outputCol="Hospital_Index")
    insurance_indexer = StringIndexer(inputCol="Insurance Provider", outputCol="Insurance_Index")

    # Fit and transform the indexers
    data_indexed = hospital_indexer.fit(data).transform(data)
    data_indexed = insurance_indexer.fit(data_indexed).transform(data_indexed)

    # Use consistent column names
    feature_columns = ["Avg_Bill_Amount", "Hospital_Index", "Insurance_Index", "Quarter"]
    logger.info(f"Feature columns used for clustering: {feature_columns}")

    # Assemble feature vector
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    data_with_features = assembler.transform(data_indexed)
    logger.info("Feature vector assembled successfully.")

    # Initialize and fit KMeans model
    kmeans = KMeans(k=9, seed=42)
    kmeans_model = kmeans.fit(data_with_features)
    logger.info("KMeans model fitted successfully.")

    # Make predictions
    predictions = kmeans_model.transform(data_with_features)
    logger.info("Predictions made successfully.")

    return predictions.select("Avg_Bill_Amount", "Hospital", "Medical Condition", "Insurance Provider", "Quarter", "prediction")  # Return relevant columns



def transform(data):
    """Transform the clustered data to include billing categories."""
    logger.info("Transforming clustered data...")

    # Calculate dynamic thresholds
    quantiles = data.approxQuantile("Avg_Bill_Amount", [0.2, 0.4, 0.6, 0.8], 0.01)
    low_threshold, moderate_low_threshold, moderate_high_threshold, high_threshold = quantiles
    logger.info(f"Calculated quantiles: {quantiles}")

    # Group by prediction and calculate average billing amount
    cluster_metadata = data.groupBy("prediction").agg(F.mean("Avg_Bill_Amount").alias("Avg_Bill_Amount"))

    logger.info("Calculating billing categories based on average billing amount...")
    cluster_metadata = cluster_metadata.withColumn(
        "BillCat",
        F.when(F.col("Avg_Bill_Amount") < low_threshold, "Very Low")
        .when((F.col("Avg_Bill_Amount") >= low_threshold) & (F.col("Avg_Bill_Amount") < moderate_low_threshold), "Low")
        .when((F.col("Avg_Bill_Amount") >= moderate_low_threshold) & (F.col("Avg_Bill_Amount") < moderate_high_threshold), "Moderate")
        .when((F.col("Avg_Bill_Amount") >= moderate_high_threshold) & (F.col("Avg_Bill_Amount") < high_threshold), "High")
        .otherwise("Very High")
    )

    # Log counts per billing category
    category_counts = cluster_metadata.groupBy("BillCat").count()
    logger.info("Counts per billing category:")
    category_counts.show()

    # Join with original data and rename columns
    data = data.join(cluster_metadata.select("prediction", "BillCat"), on="prediction", how="left")
    data = data.withColumnRenamed("Avg_Bill_Amount", "Avg Bill Amount") \
               .withColumnRenamed("BillCat", "Bill Cat")

    logger.info("Transformation completed successfully.")
    return data.drop("prediction")

def test_db_connection():
    try:
        connection = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
        connection.close()
        logger.info("Database connection successful.")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def load(data):
    """Load the processed data into the PostgreSQL database."""
    test_db_connection()

    print("Preparing to load data into the database...")
    logger.info("Preparing to load data into the database...")

    db_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    db_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    print("Writing data to the Cat Bill Amount table...")
    logger.info("Writing data to the Cat Bill Amount table...")

    try:
        print("Starting the write operation...")
        data.write.jdbc(url=db_url, table="\"Cat Bill Amount\"", mode="append", properties=db_properties)
        print("Data loaded successfully.")
        logger.info("Data loaded successfully.")
    except Exception as e:
        print(f"Failed to load data: {e}")
        logger.error(f"Failed to load data: {e}")
        raise

    print("Process completed successfully.")
    logger.info("Process completed successfully.")
    return "--- Process successfully completed! ---"

def main():
    """Main entry point for testing the functions."""
    try:
        create_table()

        extracted_data = extract()
        if extracted_data is not None and extracted_data.count() > 0:
            logger.info("Extracted Data:")
            extracted_data.show()

            clustered_data = create_model(extracted_data)
            logger.info("Clustered Data:")
            clustered_data.show()

            transformed_data = transform(clustered_data)
            logger.info("Transformed Data:")
            transformed_data.show()

            load(transformed_data)
        else:
            logger.warning("No data extracted or extracted data is empty. Exiting process.")

    except Exception as e:
        logger.error(f"An error occurred in the main function: {e}")
        raise

if __name__ == "__main__":
    main()
