from google.cloud import bigquery
from google.oauth2 import service_account

from dotenv import load_dotenv
import os

# โหลดค่าจากไฟล์ .env
load_dotenv()
# กำหนด path ของไฟล์ Service Account Key
service_account_path = os.getenv("service_account_path")

# สร้าง Credentials จากไฟล์ Service Account
credentials = service_account.Credentials.from_service_account_file(service_account_path)

# สร้าง client สำหรับเชื่อมต่อกับ BigQuery โดยใช้ credentials ที่กำหนดเอง
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# ตรวจสอบว่าเชื่อมต่อสำเร็จ
datasets = client.list_datasets()  # ดึงรายการ dataset จากโปรเจค
print("Datasets in project:")
for dataset in datasets:
    print(f"\t{dataset.dataset_id}")