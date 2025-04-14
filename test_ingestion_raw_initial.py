from dotenv import load_dotenv
import os
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from datetime import datetime


# โหลดค่าจากไฟล์ .env
load_dotenv()

# กำหนด path ของไฟล์ Service Account Key
service_account_path = os.getenv("service_account_path")

# สร้าง Credentials จากไฟล์ Service Account
credentials = service_account.Credentials.from_service_account_file(service_account_path)

# สร้าง client สำหรับเชื่อมต่อกับ BigQuery โดยใช้ credentials ที่กำหนดเอง
client_bq = bigquery.Client(credentials=credentials, project=credentials.project_id)

project_id = credentials.project_id
# Define dataset names
staging_dataset = 'wh_staging'
raw_dataset = 'wh_raw'

# Function to get all tables in the staging dataset
def get_all_tables_in_staging():
    tables = client_bq.list_tables(staging_dataset)
    return [table.table_id for table in tables]

# Function to insert data from staging table into the raw table
def insert_data_into_raw_table(staging_table_name, raw_table_name):
    query = f"""
        INSERT INTO `{raw_dataset}.{raw_table_name}` 
        (SELECT *, 
            CURRENT_TIMESTAMP() AS ingestion_datetime, 
            'snf_system' AS source_system, 
            CURRENT_TIMESTAMP() AS scd_startdate, 
            NULL AS scd_enddate
        FROM `{staging_dataset}.{staging_table_name}`)
    """
    
    # Run the query to insert data from staging to raw
    query_job = client_bq.query(query)
    query_job.result()  # Wait for the job to complete
    print(f"Inserted data from {staging_table_name} into {raw_table_name}")


def create_raw_table_from_staging(table_name):
    # Get the schema of the staging table
    staging_table_ref = client_bq.dataset(staging_dataset).table(table_name)
    staging_table = client_bq.get_table(staging_table_ref)  # Fetch the table schema
    schema = staging_table.schema  # Original schema of the table

    # Add new columns to the schema
    new_columns = [
        bigquery.SchemaField('ingestion_datetime', 'TIMESTAMP', mode='REQUIRED'),
        bigquery.SchemaField('source_system', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('scd_startdate', 'TIMESTAMP', mode='REQUIRED'),
        bigquery.SchemaField('scd_enddate', 'TIMESTAMP', mode='NULLABLE')  # NULLABLE, to indicate open-ended
    ]

    # Combine the original schema with the new columns
    full_schema = schema + new_columns

    # Create the new table reference with the "snf_" prefix
    new_table_name = f"snf_{table_name}"
    new_table_ref = client_bq.dataset(raw_dataset).table(new_table_name)

    # Create the new table with the full schema
    table = bigquery.Table(new_table_ref, schema=full_schema)
    client_bq.create_table(table)  # Create the table in BigQuery
    print(f"Created table {new_table_name} in raw dataset")

    # Insert data from staging to raw dataset
    insert_data_into_raw_table(table_name, new_table_name)

# Main function to loop through all tables and create corresponding tables in the raw dataset
def main():
    # Step 1: Get all tables from the staging dataset
    tables_in_staging = get_all_tables_in_staging()

    # Step 2: Loop through each table and create the corresponding table in the raw dataset
    for table_name in tables_in_staging:
        create_raw_table_from_staging(table_name)

# Run the main function
main()