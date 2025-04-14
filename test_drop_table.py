import pandas as pd
from dotenv import load_dotenv
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict



# โหลดค่าจากไฟล์ .env
load_dotenv()

# Set your dataset ID
dataset_id = 'wh_staging'

# กำหนด path ของไฟล์ Service Account Key
service_account_path = os.getenv("service_account_path")

# สร้าง Credentials จากไฟล์ Service Account
credentials = service_account.Credentials.from_service_account_file(service_account_path)

# สร้าง client สำหรับเชื่อมต่อกับ BigQuery โดยใช้ credentials ที่กำหนดเอง
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

def drop_all_tables_in_dataset(client, dataset_id):
    # Get the list of tables in the dataset
    tables = client.list_tables(dataset_id)
    
    for table in tables:
        # Get the table reference
        table_ref = client.dataset(dataset_id).table(table.table_id)
        
        # Attempt to delete the table
        try:
            client.delete_table(table_ref)  # Delete the table
            print(f"Deleted table: {table.table_id}")
        except Exception as e:
            print(f"Failed to delete table {table.table_id}: {e}")


# Call the function to drop all tables in the dataset
drop_all_tables_in_dataset(client, dataset_id)