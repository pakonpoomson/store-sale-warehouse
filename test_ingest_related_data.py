import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict
import pyarrow as pa
import pyarrow.parquet as pq
from Utils import *

# โหลดค่าจากไฟล์ .env
load_dotenv()

parquet_file = '/tmp/data_file.parquet'
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
client_bq = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Config JSON array for multiple tables
config = [
    {
        "bigquery_table": "DATE_DIM",  # BigQuery table
        "snowflake_table": "DATE_DIM",  # Snowflake table
        "bigquery_column": "SS_SOLD_DATE_SK",  # BigQuery column
        "snowflake_column": "D_DATE_SK"  # Snowflake column
    },
    {
        "bigquery_table": "TIME_DIM",
        "snowflake_table": "TIME_DIM",
        "bigquery_column": "SS_SOLD_TIME_SK",
        "snowflake_column": "T_TIME_SK"
    },
    {
        "bigquery_table": "ITEM",
        "snowflake_table": "ITEM",
        "bigquery_column": "SS_ITEM_SK",
        "snowflake_column": "I_ITEM_SK"
    },
    {
        "bigquery_table": "CUSTOMER",
        "snowflake_table": "CUSTOMER",
        "bigquery_column": "SS_CUSTOMER_SK",
        "snowflake_column": "C_CUSTOMER_SK"
    },
    {
        "bigquery_table": "CUSTOMER_DEMOGRAPHICS",
        "snowflake_table": "CUSTOMER_DEMOGRAPHICS",
        "bigquery_column": "SS_CDEMO_SK",
        "snowflake_column": "CD_DEMO_SK"
    }
    # Add more tables as needed
]

# Step 1: Query distinct values from BigQuery table
def get_distinct_values_from_bq(bigquery_column):
    query = f"""
        SELECT DISTINCT {bigquery_column}
        FROM `{dataset_id}.STORE_SALES`
        WHERE {bigquery_column} is not null
    """
    query_job = client_bq.query(query)
    result = query_job.result()
    
    # Collecting distinct values into a list
    distinct_values = [row[0] for row in result]
    return distinct_values

# Step 2: Construct WHERE condition for Snowflake query using the IN clause
def construct_where_condition(snowflake_column, distinct_values):
    if distinct_values:
        values_str = ", ".join([f"'{value}'" if isinstance(value, str) else str(value) for value in distinct_values])
        condition = f"{snowflake_column} IN ({values_str})"
        return condition
    return ""

# Step 3: Query Snowflake with the WHERE condition
def export_data_from_snowflake(snowflake_table, where_condition, bigquery_table):
    if where_condition:
        snowflake_query = f"""
            SELECT *
            FROM {snowflake_table}
            WHERE {where_condition}
        """
        print(snowflake_query)
        # Execute the query in Snowflake
        cursor = conn.cursor()
        cursor.execute(snowflake_query)
        # Fetch the data into a pandas DataFrame
        df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        # Get the BigQuery table schema

        bq_schema = get_bq_schema(client_bq, dataset_id, bigquery_table)
        # Adjust DataFrame columns to match BigQuery schema
        df_adjusted, ref_schema = adjust_dataframe_types(df, bq_schema)
        # Convert the DataFrame to a Parquet file using PyArrow
        table = pa.Table.from_pandas(df_adjusted, schema=ref_schema)
        # Save Arrow Table as Parquet file locally
       
        pq.write_table(table, parquet_file)

    return []

# Step 4: Insert data into BigQuery

def insert_data_into_bq(table_name):
# Open the Parquet file for upload
# Define the BigQuery table reference
    table_ref = client_bq.dataset(dataset_id).table(table_name)
    with open(parquet_file, 'rb') as f:
        # Load the Parquet file into BigQuery
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,  # Automatically detect schema from Parquet
        )

        load_job = client_bq.load_table_from_file(f, table_ref, job_config=job_config)

        # Wait for the load job to complete
    load_job.result()

    print(f"Loaded {load_job.output_rows} rows into {table_name}")


# Main function to loop through all tables and upsert data into BigQuery
def main():
    for table_info in config:
        bigquery_table = table_info["bigquery_table"]
        snowflake_table = table_info["snowflake_table"]
        bigquery_column = table_info["bigquery_column"]
        snowflake_column = table_info["snowflake_column"]
        
        # Step 1: Get distinct values from BigQuery
        distinct_values = get_distinct_values_from_bq(bigquery_column)
        
        # Step 2: Construct WHERE condition for Snowflake query
        where_condition = construct_where_condition(snowflake_column, distinct_values)
        
        # Step 3: Export data from Snowflake using the WHERE condition
        export_data_from_snowflake(snowflake_table, where_condition, bigquery_table)
        
        # Step 4: Insert data into BigQuery
        insert_data_into_bq(bigquery_table)

# Run the main function
main()