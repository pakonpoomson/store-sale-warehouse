# test_ingestion_staging_incremental.py
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os
import json
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound # Import NotFound exception
from google.oauth2 import service_account
from io import StringIO
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from Utils import get_bq_schema, adjust_dataframe_types # Assuming Utils.py is correct

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
    if conn: conn.close() # Close snowflake connection if open
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
gcs_folder = 'snowflake_incremental' # Consider a different folder for incremental loads
project_id = credentials.project_id
dataset_staging_id = 'wh_staging'
dataset_raw_id = 'wh_raw' # The dataset where the script LOOKS for max timestamps
source_prefix = 'snf_' # Prefix added to table name when checking in raw dataset
temp_parquet_dir = '/tmp' # Local directory for temporary parquet files

# Ensure the temporary directory exists
os.makedirs(temp_parquet_dir, exist_ok=True)


# Read the JSON configuration file
def load_json_config(json_file):
    try:
        with open(json_file, 'r') as file:
            config = json.load(file)
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file '{json_file}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{json_file}'.")
        return None

# Function to get the max timestamp from the BigQuery destination table
def get_max_timestamp_from_bq(table_name, timestamp_columns):
    """
    Fetches the maximum value for specified timestamp columns from the target table in BigQuery.
    Handles the case where the table does not exist (returns None values).
    """
    # **Important**: This checks the 'raw' table, which seems inconsistent
    # with where the initial load script places data.
    # Consider changing dataset_raw_id and source_prefix if needed.
    target_table_id = f"{project_id}.{dataset_raw_id}.{source_prefix}{table_name}"

    max_timestamps = {col: None for col in timestamp_columns} # Initialize with None

    if not timestamp_columns:
        print(f"No timestamp columns defined for {table_name}. Skipping max timestamp check.")
        return max_timestamps # Return dict with None values

    # Generate the `MAX()` condition for each timestamp column
    timestamp_conditions = ', '.join([f"MAX({col}) AS max_{col}" for col in timestamp_columns])

    # Construct the query to get max timestamps from BigQuery
    query = f"""
        SELECT {timestamp_conditions}
        FROM `{target_table_id}`
    """
    print(f"Executing query to find max timestamps in BQ: {query}")

    try:
        query_job = client_bq.query(query)
        result = query_job.result() # Wait for the job to complete

        # Should be only one row in the result
        for row in result:
            for col in timestamp_columns:
                max_timestamps[col] = row[f"max_{col}"]
            # Assuming only one row of max values, break after the first
            break

        print(f"Found max timestamps for {target_table_id}: {max_timestamps}")

    except NotFound:
        print(f"Warning: BigQuery table {target_table_id} not found. Assuming initial load or no previous data.")
        # Keep max_timestamps as the initial dictionary with None values
    except Exception as e:
        print(f"Error querying max timestamps from BigQuery table {target_table_id}: {e}")
        # Keep max_timestamps as the initial dictionary with None values, or handle error differently

    return max_timestamps


# Function to export new data from Snowflake
def export_new_data_from_snowflake(table_name, timestamp_columns, max_timestamps):
    """
    Exports data from Snowflake newer than the provided max_timestamps.
    If max_timestamps contains None, fetches data without timestamp filter (limited).
    """
    cursor = conn.cursor()
    df = pd.DataFrame() # Initialize empty DataFrame
    export_date = datetime.now().strftime("%Y%m%d_%H%M%S") # Add time for uniqueness
    file_name = f"{table_name}_{export_date}_incremental.parquet"
    local_file_path = os.path.join(temp_parquet_dir, file_name)
    gcs_file_path = f"{gcs_folder}/{file_name}"

    # Construct the WHERE clause based on max_timestamps
    where_clauses = []
    perform_full_load = False # Flag to determine if we need a full load (due to missing timestamps)

    # Check if any max timestamp is None. If so, we can't do a reliable incremental load based on time.
    if any(max_timestamps.get(col) is None for col in timestamp_columns):
         print(f"Warning: Max timestamp not found for one or more columns in {table_name}. Fetching initial batch (LIMIT 100).")
         perform_full_load = True
    else:
        for col in timestamp_columns:
            max_val = max_timestamps.get(col)
            # Basic type check - adjust quoting as needed for your Snowflake data types
            # Assuming numeric keys don't need quotes, date/timestamp might.
            # This might need refinement based on actual Snowflake data types.
            if isinstance(max_val, (datetime, pd.Timestamp, pd.Period)):
                 # Format timestamp/date appropriately for Snowflake SQL
                 formatted_val = max_val.strftime('%Y-%m-%d %H:%M:%S.%f') # Example format
                 where_clauses.append(f'"{col}" > \'{formatted_val}\'') # Quote column name if needed
            elif isinstance(max_val, str):
                 where_clauses.append(f'"{col}" > \'{max_val}\'') # Quote column name if needed
            else: # Assume numeric
                 where_clauses.append(f'"{col}" > {max_val}') # Quote column name if needed

    # Build the final query
    query_base = f'SELECT * FROM "{snowflake_schema}"."{table_name}"' # Use qualified name
    if not perform_full_load and where_clauses:
        query = f"{query_base} WHERE {' AND '.join(where_clauses)} LIMIT 100" # Limit for safety/testing
    else:
        # If it's effectively a full load or no timestamp filter, just limit
        query = f"{query_base} LIMIT 100" # Limit for safety/testing

    print(f"Executing Snowflake query for {table_name}: {query}")

    try:
        cursor.execute(query)
        # Fetch the data into a pandas DataFrame
        df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        print(f"Fetched {len(df)} rows from {table_name}.")

        if df.empty:
            print(f"No new data found for {table_name} based on the criteria. Skipping GCS upload and BQ load.")
            return None # Indicate no file was generated

        # --- Schema Handling and Parquet Conversion ---
        # Get the schema from the *staging* table (where we are loading)
        try:
             bq_staging_table_id = f"{project_id}.{dataset_staging_id}.{table_name}"
             bq_staging_table_ref = client_bq.get_table(bq_staging_table_id)
             bq_schema = bq_staging_table_ref.schema
             print(f"Using schema from existing BQ staging table: {bq_staging_table_id}")
             # Adjust DataFrame columns to match BigQuery schema
             df_adjusted, ref_schema = adjust_dataframe_types(df, bq_schema)
             # Convert the DataFrame to a Parquet file using PyArrow with explicit schema
             table = pa.Table.from_pandas(df_adjusted, schema=ref_schema, preserve_index=False)

        except NotFound:
            print(f"Warning: BigQuery staging table {bq_staging_table_id} not found.")
            print("Will attempt Parquet conversion with schema inference from pandas DataFrame.")
            # Fallback to schema inference if staging table doesn't exist
            # Note: This might lead to type mismatches if BQ later requires a specific schema
            table = pa.Table.from_pandas(df, preserve_index=False) # Let PyArrow infer schema

        except Exception as e_schema:
             print(f"Error getting BQ schema or adjusting types for {table_name}: {e_schema}")
             print("Proceeding with schema inference, but potential issues may arise.")
             table = pa.Table.from_pandas(df, preserve_index=False)

        # Print schema being written to Parquet
        print(f"Schema for Parquet file ({file_name}):\n{table.schema}")

        # Write Parquet file locally
        pq.write_table(table, local_file_path)
        print(f"Successfully wrote Parquet file locally: {local_file_path}")

        # --- Upload to GCS ---
        print(f"Uploading {local_file_path} to GCS bucket {gcs_bucket_name} at {gcs_file_path}...")
        bucket = client_gcs.get_bucket(gcs_bucket_name)
        blob = bucket.blob(gcs_file_path)
        blob.upload_from_filename(local_file_path)

        print(f"Successfully exported new data for {table_name} to GCS: gs://{gcs_bucket_name}/{gcs_file_path}")
        return gcs_file_path # Return GCS path

    except snowflake.connector.errors.ProgrammingError as sf_err:
        print(f"Snowflake Error exporting table {table_name}: {sf_err}")
        return None # Indicate failure
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

# Function to load the exported Parquet file into BigQuery Staging
def load_parquet_to_bq_staging(gcs_file_path, table_name):
    """Loads data from a GCS Parquet file into the BigQuery staging table."""
    if not gcs_file_path:
        print(f"Skipping BigQuery load for {table_name} because GCS file path is missing.")
        return

    # Define the BigQuery staging table reference
    table_id = f"{project_id}.{dataset_staging_id}.{table_name}" # Load into staging
    table_ref = bigquery.TableReference.from_string(table_id)

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        # Use autodetect=True IF the staging table might not exist yet
        # or if you want BQ to define the schema from the Parquet file.
        # If the staging table MUST exist with a predefined schema, set autodetect=False.
        autodetect=True,
        # Choose write disposition carefully:
        # WRITE_TRUNCATE: Overwrites the staging table completely.
        # WRITE_APPEND: Adds data to the staging table.
        # WRITE_EMPTY: Fails if the staging table is not empty.
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # Example: Overwrite staging
    )

    gcs_uri = f"gs://{gcs_bucket_name}/{gcs_file_path}"
    print(f"Starting BigQuery load job from {gcs_uri} to {table_id}...")

    try:
        load_job = client_bq.load_table_from_uri(
            gcs_uri, table_ref, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete

        destination_table = client_bq.get_table(table_ref)
        print(f"Loaded {destination_table.num_rows} rows into BigQuery staging table {table_id}.")

    except Exception as e:
        print(f"Error loading data from GCS {gcs_uri} to BigQuery table {table_id}: {e}")


# Main workflow
def main(json_config_file):
    """Main function to orchestrate the incremental load process."""
    print("Starting Snowflake to BigQuery incremental ingestion process...")

    # Load JSON configuration
    config = load_json_config(json_config_file)
    if config is None:
        print("Exiting due to configuration loading error.")
        if conn: conn.close()
        return

    for table_info in config:
        table_name = table_info.get('table_name')
        timestamp_columns = table_info.get('timestamp_columns', []) # Default to empty list

        if not table_name:
            print("Skipping entry due to missing 'table_name' in config:", table_info)
            continue

        print(f"\n--- Processing table: {table_name} ---")

        # Step 1: Get the max timestamp from the *destination/raw* BigQuery table
        # *** Logic Alert: This checks wh_raw.snf_TABLENAME ***
        max_timestamps = get_max_timestamp_from_bq(table_name, timestamp_columns)

        # Step 2: Export new data from Snowflake based on the max timestamp
        gcs_file_path = export_new_data_from_snowflake(table_name, timestamp_columns, max_timestamps)

        # Step 3: Load the exported Parquet file into the BigQuery *staging* table
        if gcs_file_path: # Only load if export was successful and produced a file
            load_parquet_to_bq_staging(gcs_file_path, table_name)
        else:
            print(f"Skipping BigQuery staging load for {table_name} as export failed or produced no file.")

    # Close the Snowflake connection
    if conn:
        conn.close()
        print("\nSnowflake connection closed.")

    print("Incremental ingestion process finished.")

# Run the main function with the JSON config file
if __name__ == "__main__":
    main('tables_config.json') # Make sure this file exists