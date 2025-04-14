import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os

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

# กำหนดคำสั่ง SQL เพื่อดึงข้อมูลที่แปลงแล้ว
query = "select * from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.STORE_SALES limit 10;"  # แทนที่ด้วยตารางหรือโมเดลที่แปลงแล้วของคุณ

# รันคำสั่ง SQL และดึงข้อมูลมาที่ pandas DataFrame
df = pd.read_sql(query, conn)
print(df)

# ปิดการเชื่อมต่อกับ Snowflake
conn.close()