import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict

# โหลดค่าจากไฟล์ .env
load_dotenv()

# สร้างการเชื่อมต่อกับ Snowflake
conn = snowflake.connector.connect(
    user= os.getenv("user"),
    password= os.getenv("password"),
    account= os.getenv("account"),
    warehouse= os.getenv("warehouse"),
    database= os.getenv("database"),
    schema= os.getenv("schema")
)

snowflake_schema = 'TPCDS_SF10TCL'
dataset_id = 'wh_staging'

# กำหนด path ของไฟล์ Service Account Key
service_account_path = os.getenv("service_account_path")

# สร้าง Credentials จากไฟล์ Service Account
credentials = service_account.Credentials.from_service_account_file(service_account_path)

# สร้าง client สำหรับเชื่อมต่อกับ BigQuery โดยใช้ credentials ที่กำหนดเอง
client = bigquery.Client(credentials=credentials, project=credentials.project_id)


def remove_duplicate_columns(schema):
    # Create a dictionary to store unique columns by name
    unique_columns = {}
    
    # Loop through the schema and add unique columns to the dictionary
    for field in schema:
        if field.name not in unique_columns:
            unique_columns[field.name] = field
    
    # Return the list of unique columns
    return list(unique_columns.values())

def get_snowflake_table_schema(table_name):
    # Query to get the schema of the table from Snowflake
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
    """
    cursor = conn.cursor()
    cursor.execute(query)
    columns = cursor.fetchall()
    
    # Prepare the schema for BigQuery
    bq_schema = []
    for column in columns:
        column_name, data_type = column
        bq_schema.append(bigquery.SchemaField(column_name, convert_to_bq_type(data_type)))
    cursor.close()
    return bq_schema

def convert_to_bq_type(snowflake_data_type):
    # Map Snowflake data types to BigQuery data types
    type_map = {
        'STRING': 'STRING',
        'VARCHAR': 'STRING',
        'TEXT': 'STRING',
        'NUMBER': 'FLOAT',
        'INTEGER': 'INTEGER',
        'BOOLEAN': 'BOOLEAN',
        'DATE': 'DATE',
        'TIMESTAMP': 'TIMESTAMP',
        'TIMESTAMP_LTZ': 'TIMESTAMP',
        'TIMESTAMP_TZ': 'TIMESTAMP',
        'TIMESTAMP_NTZ': 'TIMESTAMP',
        'FLOAT': 'FLOAT',
        'DOUBLE': 'FLOAT',
        'DECIMAL': 'FLOAT',
        'REAL': 'FLOAT'
    }
    return type_map.get(snowflake_data_type.upper(), 'STRING')  # Default to STRING if type not found

def create_empty_table_in_bq(table_name, schema):
     # Define the BigQuery dataset reference
    dataset_ref = client.dataset(dataset_id)
    
    # Define the table reference within the dataset
    table_ref = dataset_ref.table(table_name)

    schema = remove_duplicate_columns(schema)

    # Define the table schema
        # Check if the table already exists
    try:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)  # Attempt to create the table
        print(f"Created table {table_name} in BigQuery.")
    except Conflict:  # If the table already exists
        print(f"Table {table_name} already exists in BigQuery.")

def sync_tables_from_snowflake_to_bigquery():
    # Query to get all tables in the Snowflake schema
    query = f"""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{snowflake_schema}'
    """
    cursor = conn.cursor()
    cursor.execute(query)
    tables = cursor.fetchall()
    
    
    # Loop through each table and create the same structure in BigQuery
    for table in tables:
        table_name = table[0]
        print(f"Syncing table {table_name}...")

        # Get the schema for the current table from Snowflake
        schema = get_snowflake_table_schema(table_name)
        print(schema)

        # Create an empty table in BigQuery with the same schema
        create_empty_table_in_bq(table_name, schema)

    cursor.close()

# Run the sync process
sync_tables_from_snowflake_to_bigquery()

# Close the Snowflake connection
conn.close()