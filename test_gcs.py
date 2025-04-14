from google.cloud import storage
from google.oauth2 import service_account

from dotenv import load_dotenv
import os

# โหลดค่าจากไฟล์ .env
load_dotenv()
# กำหนด path ของไฟล์ Service Account Key
service_account_path = os.getenv("service_account_path")

# สร้าง Credentials จากไฟล์ Service Account
credentials = service_account.Credentials.from_service_account_file(service_account_path)

# สร้าง client สำหรับเชื่อมต่อกับ GCS โดยใช้ credentials ที่กำหนดเอง
client = storage.Client(credentials=credentials, project=credentials.project_id)

# ลิสต์ bucket ทั้งหมดในโปรเจค
buckets = client.list_buckets()

# แสดงชื่อ bucket
print("Buckets in project:")
for bucket in buckets:
    print(bucket.name)

# การอัปโหลดไฟล์ไปยัง GCS
bucket_name = "9a8b7c6d-data-landingzone"
bucket = client.get_bucket(bucket_name)

# ชื่อไฟล์ที่ต้องการอัปโหลด
blob = bucket.blob("uploaded_file.txt")
blob.upload_from_filename("local_file.txt")

print("File uploaded successfully!")

# การดาวน์โหลดไฟล์จาก GCS
blob.download_to_filename("downloaded_file.txt")
print("File downloaded successfully!")