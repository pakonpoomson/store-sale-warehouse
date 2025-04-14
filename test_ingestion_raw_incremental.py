from dotenv import load_dotenv
import os
import json
from google.cloud import storage, bigquery
from google.oauth2 import service_account


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


# Read the JSON configuration file
def load_json_config(json_file):
    with open(json_file, 'r') as file:
        config = json.load(file)
    return config

# Function to get data from the staging dataset and apply upsert logic with CTEs
def upsert_data_with_cte(table_name, pk_column, source):
    # Build the SQL query using CTEs to perform SCD Type 2 logic
    if "|" in pk_column:
        pk_columns = pk_column.split('|')
        pk_condition = " AND ".join([f"raw.{col} = staging.{col}" for col in pk_columns])
    else:
        pk_condition = f"raw.{pk_column} = staging.{pk_column}"

    query = f"""
        -- Update existing records' scd_enddate to current timestamp
        UPDATE `{raw_dataset}.{source}_{table_name}` raw
        SET raw.scd_enddate = CURRENT_TIMESTAMP()
         WHERE EXISTS (
            SELECT 1
            FROM `{staging_dataset}.{table_name}` staging
            WHERE {pk_condition} AND raw.scd_enddate IS NULL
        )
        
    """
    
    # Run the query
    print(query)
    query_job = client_bq.query(query)
    query_job.result()  # Wait for the job to complete

    query = f"""
        -- Insert new records from staging dataset
        INSERT INTO `{raw_dataset}.{source}_{table_name}` 
        SELECT *, 
               CURRENT_TIMESTAMP() AS ingestion_datetime, 
               'snf_system' AS source_system, 
               CURRENT_TIMESTAMP() AS scd_startdate, 
               NULL AS scd_enddate
        FROM `{staging_dataset}.{table_name}` 
    """
    
    # Run the query
    print(query)
    query_job = client_bq.query(query)
    query_job.result()  # Wait for the job to complete
    print(f"Upserted data into {table_name} in raw dataset")

# Main function to loop through all tables and upsert data into the raw dataset
def main(json_file):
    config = load_json_config(json_file)

    for table_info in config:
        table_name = table_info['table_name']
        pk_column = table_info['pk_column']  # Assuming single column PK for simplicity
        source = table_info['source']
        # Perform upsert operation using CTEs
        upsert_data_with_cte(table_name, pk_column, source)

# Run the main function
main('tables_config.json')