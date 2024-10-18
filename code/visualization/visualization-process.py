import decimal
import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from bokeh.io import output_file, save
from bokeh.models import ColumnDataSource, DataTable, TableColumn
from bokeh.layouts import column

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load environment variables from .env file
load_dotenv()

# Database configuration from environment variables
db_host = os.getenv('DB_HOST')
db_name = os.getenv('POSTGRES_DB')
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')

# Print database connection info (for debugging)
logging.info("Database connection details:")
logging.info(f"Host: {db_host}")
logging.info(f"Database: {db_name}")
logging.info(f"User: {db_user}")

# Create Spark session
spark = SparkSession.builder \
    .appName('Data_Engineering_Project') \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
    .getOrCreate()


logging.info("Spark session created successfully.")

# Ensure the output directory exists
output_dir = "htdocs"
os.makedirs(output_dir, exist_ok=True)
logging.info(f"Output directory checked/created: {output_dir}")


def fetch_data_from_db(query):
    '''Fetches data from the database using the provided SQL query.'''
    jdbc_url = f"jdbc:postgresql://{db_host}/{db_name}"
    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    logging.info("Connecting to the database...")
    data = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

    if data.count() == 0:
        raise ValueError("No data found in the DataFrame.")

    logging.info(f"Data loaded successfully. Number of rows: {data.count()}")
    return data.toPandas()


def convert_decimal_columns(df):
    '''Converts Decimal columns in the DataFrame to float.'''
    return df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)


def create_data_table(df, output_filename):
    '''Creates a Bokeh DataTable and saves it to an HTML file.'''
    source = ColumnDataSource(df)
    columns = [TableColumn(field=col, title=col) for col in df.columns]

    data_table = DataTable(source=source, columns=columns, width=800, height=800)
    output_file(output_filename)
    layout = column(data_table)

    save(layout)
    logging.info(f"Visualization saved to {output_filename}")


def get_ml_result():
    '''Fetches the machine learning results from the database.'''
    sql_query = '"Cat Bill Amount"'
    try:
        pandas_df = fetch_data_from_db(sql_query)
        pandas_df = convert_decimal_columns(pandas_df)
        create_data_table(pandas_df, f"{output_dir}/mlresult.html")
    except Exception as e:
        logging.error(f"An error occurred while fetching ML results: {e}")


def get_etl_result():
    '''Fetches the ETL results from the database.'''
    sql_query = """
    (SELECT 
        "Medical Condition", 
        "Hospital", 
        "Insurance Provider", 
        "Quarter", 
        CAST("Discharge Date" AS TEXT) AS discharge_date,
        "Average Bill Amount" 
    FROM admission) AS subquery
    """
    try:
        pandas_df = fetch_data_from_db(sql_query)
        pandas_df = convert_decimal_columns(pandas_df)
        create_data_table(pandas_df, f"{output_dir}/etlresult.html")
    except Exception as e:
        logging.error(f"An error occurred while fetching ETL results: {e}")


def get_status():
    '''Fetches the status information from the database.'''
    sql_query = """
    (SELECT 
        id, 
        status, 
        message, 
        CAST(timestamp AS TEXT) AS timestamp, 
        CAST(lastloaded AS TEXT) AS lastloaded 
    FROM status) AS subquery
    """
    try:
        pandas_df = fetch_data_from_db(sql_query)
        create_data_table(pandas_df, f"{output_dir}/status.html")
    except Exception as e:
        logging.error(f"An error occurred while fetching status information: {e}")


# Execute all functions to generate reports
get_ml_result()
get_etl_result()
get_status()

# Stop Spark session
spark.stop()
logging.info("Spark session stopped.")
