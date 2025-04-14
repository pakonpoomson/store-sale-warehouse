# test_ingestion_staging_initial.py
import os
import snowflake.connector
from dotenv import load_dotenv
from google.cloud import storage, bigquery
from google.oauth2 import service_account
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import pandas as pd
from Utils import get_bq_schema, adjust_dataframe_types

# โหลดค่าจากไฟล์ .env
load_dotenv()

# --- Snowflake Connection ---
try:
    conn = snowflake.connector.connect(
        user=os.getenv("user"),
        password=os.getenv("password"),
        account=os.getenv("account"),
        warehouse=os.getenv("warehouse"),
        database=os.getenv("database"),
        schema=os.getenv("schema") # Use the general schema for connection
    )
except Exception as e:
    print(f"Error connecting to Snowflake: {e}")
    exit() # Exit if connection fails

# Define the specific schema to query tables from
snowflake_schema = 'TPCDS_SF10TCL' # Schema containing the tables to be exported

# --- Google Cloud Setup ---
service_account_path = os.getenv("service_account_path")
if not service_account_path or not os.path.exists(service_account_path):
    print(f"Error: Service account file not found at path: {service_account_path}")
    exit()

try:
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    client_bq = bigquery.Client(credentials=credentials, project=credentials.project_id)
    client_gcs = storage.Client(credentials=credentials, project=credentials.project_id)
except Exception as e:
    print(f"Error creating Google Cloud clients: {e}")
    if conn: conn.close() # Close snowflake connection if open
    exit()

# --- Configuration ---
gcs_bucket_name = '9a8b7c6d-data-landingzone'
gcs_folder = 'snowflake'
project_id = credentials.project_id
dataset_id = 'wh_staging'
temp_parquet_dir = '/tmp' # Local directory for temporary parquet files

# Ensure the temporary directory exists
os.makedirs(temp_parquet_dir, exist_ok=True)

# --- Functions ---

def get_all_tables_from_snowflake():
    """Fetches all table names from the specified Snowflake schema."""
    query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{snowflake_schema}' AND TABLE_TYPE = 'BASE TABLE'" # Ensure only tables, not views
    tables = []
    cursor = conn.cursor()
    try:
        print(f"Fetching tables from Snowflake schema: {snowflake_schema}")
        cursor.execute(query)
        tables_raw = cursor.fetchall()
        tables = [table[0] for table in tables_raw]
        print(f"Found tables: {tables}")
    except Exception as e:
        print(f"Error fetching tables from Snowflake: {e}")
    finally:
        cursor.close()
    return tables

def export_table_to_gcs_as_parquet(table_name):
    """Exports data from a Snowflake table to GCS as a Parquet file."""
    export_date = datetime.now().strftime("%Y%m%d")
    file_name = f"{table_name}_{export_date}.parquet"
    local_file_path = os.path.join(temp_parquet_dir, file_name)
    gcs_file_path = f"{gcs_folder}/{file_name}"

    # Query to get the first 100 rows (or adjust as needed for testing)
    # For full export, remove LIMIT 100
    query = f'SELECT * FROM "{snowflake_schema}"."{table_name}" LIMIT 100' # Use qualified name and quotes if needed
    cursor = conn.cursor()

    try:
        print(f"Executing query for table {table_name}: {query}")
        cursor.execute(query)

        # Fetch the data into a pandas DataFrame
        df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        print(f"Fetched {len(df)} rows from {table_name}.")

        if df.empty:
             print(f"Table {table_name} is empty or query returned no results. Skipping export.")
             return None # Return None if no data to prevent empty file upload

        # --- MODIFICATION START ---
        # Removed the call to get_bq_schema and adjust_dataframe_types
        # We will let BigQuery auto-detect the schema during load.
        # Convert the DataFrame to a Parquet file using PyArrow, inferring schema
        print(f"Converting DataFrame for {table_name} to Parquet format...")
        table = pa.Table.from_pandas(df, preserve_index=False) # Let PyArrow infer schema
        pq.write_table(table, local_file_path)
        print(f"Successfully wrote Parquet file locally: {local_file_path}")
        # --- MODIFICATION END ---

        # Upload the Parquet file to Google Cloud Storage
        print(f"Uploading {local_file_path} to GCS bucket {gcs_bucket_name} at {gcs_file_path}...")
        bucket = client_gcs.get_bucket(gcs_bucket_name)
        blob = bucket.blob(gcs_file_path)
        blob.upload_from_filename(local_file_path)

        print(f"Successfully exported {table_name} to GCS: gs://{gcs_bucket_name}/{gcs_file_path}")
        return file_name # Return the GCS file name (not path)

    except snowflake.connector.errors.ProgrammingError as sf_err:
         print(f"Snowflake Error exporting table {table_name}: {sf_err}")
         # Check if the error indicates the table doesn't exist (e.g., error code 2003)
         if "does not exist or not authorized" in str(sf_err):
              print(f"Skipping table {table_name} as it might not exist or access is denied.")
         return None # Skip this table
    except Exception as e:
        print(f"Error during export for table {table_name}: {e}")
        return None # Indicate failure
    finally:
        cursor.close()
        # Clean up the local temporary file
        if os.path.exists(local_file_path):
            try:
                os.remove(local_file_path)
                print(f"Removed temporary local file: {local_file_path}")
            except OSError as e_remove:
                print(f"Error removing temporary file {local_file_path}: {e_remove}")


def load_gcs_to_bq(table_name, file_name):
    """Loads data from a GCS Parquet file into a BigQuery table."""
    if not file_name:
        print(f"Skipping BigQuery load for {table_name} due to missing GCS file.")
        return

    gcs_uri = f"gs://{gcs_bucket_name}/{gcs_folder}/{file_name}"
    table_id = f"{project_id}.{dataset_id}.{table_name}" # Use full table ID

    # Define the BigQuery table reference
    # table_ref = client_bq.dataset(dataset_id).table(table_name) # Original way
    table_ref = bigquery.TableReference.from_string(table_id) # More explicit way

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,  # Automatically detect schema from Parquet file
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # Overwrite table if exists, create if not. Use WRITE_APPEND to add data.
    )

    print(f"Starting BigQuery load job from {gcs_uri} to {table_id}...")
    try:
        load_job = client_bq.load_table_from_uri(
            gcs_uri, table_ref, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete

        destination_table = client_bq.get_table(table_ref)
        print(f"Loaded {destination_table.num_rows} rows into BigQuery table {table_id}.")

    except Exception as e:
        print(f"Error loading data from GCS {gcs_uri} to BigQuery table {table_id}: {e}")

def main():
    """Main function to orchestrate the export and load process."""
    print("Starting Snowflake to BigQuery ingestion process...")

    # STEP 1: Get the list of tables from Snowflake
    tables = get_all_tables_from_snowflake()

    if not tables:
        print("No tables found in Snowflake schema or error fetching tables. Exiting.")
        if conn: conn.close()
        return

    tables_to_process = tables # Process all tables found for now

    for table_name in tables_to_process:
        print(f"\n--- Processing table: {table_name} ---")
        # STEP 2: Export the table to GCS as Parquet
        gcs_file_name = export_table_to_gcs_as_parquet(table_name)

        # STEP 3: Load the exported data from GCS to BigQuery
        if gcs_file_name: # Only load if export was successful
             load_gcs_to_bq(table_name, gcs_file_name)
        else:
             print(f"Skipping BigQuery load for {table_name} as export failed or produced no file.")


    # Close the Snowflake connection
    if conn:
        conn.close()
        print("\nSnowflake connection closed.")

    print("Ingestion process finished.")

# Run the main function
if __name__ == "__main__":
    main()