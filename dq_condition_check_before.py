import os
import snowflake.connector
from dotenv import load_dotenv
import pandas as pd
import json
# สร้างการเชื่อมต่อกับ Snowflake
conn = snowflake.connector.connect(
    user= os.getenv("user"),
    password= os.getenv("password"),
    account= os.getenv("account"),
    warehouse= os.getenv("warehouse"),
    database= os.getenv("database"),
    schema= os.getenv("schema")
)

# Read the JSON configuration file
def load_json_config(json_file):
    with open(json_file, 'r') as file:
        config = json.load(file)
    return config

snowflake_schema = 'TPCDS_SF10TCL'
# Function to check if table exists
def table_exists(connection, table_name):
    query = f"SHOW TABLES LIKE '{table_name}'"
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return len(result) > 0

# Function to check if columns exist in the table
def columns_exist(connection, table_name, columns):
    query = f"DESCRIBE TABLE {table_name}"
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    existing_columns = [row[0] for row in result]  # The first column of the result contains column names
    return all(col in existing_columns for col in columns)

# Function to check the row count deviation
def row_count_valid_SS(connection, table_name, expected_date, expected_row_group, expected_row_count, deviation_percent):
    query = f"SELECT COUNT(*) FROM {snowflake_schema}.{table_name} ss left join date_dim dd on ss.SS_SOLD_DATE_SK = dd.d_date_sk where dd.d_date = '{expected_date}'"
    print(query)
    cursor = connection.cursor()
    cursor.execute(query)
    row_count = cursor.fetchone()[0]
    cursor.close()
    min_row_count = expected_row_count - (expected_row_count * deviation_percent / 100)
    max_row_count = expected_row_count + (expected_row_count * deviation_percent / 100)
    return min_row_count <= row_count <= max_row_count

# Function to check the row count deviation
def row_count_valid(connection, table_name, expected_date, expected_row_group, expected_row_count, deviation_percent):
    query = f"SELECT COUNT(*) FROM {snowflake_schema}.{table_name} where {expected_row_group} = '{expected_date}'"
    print(query)
    cursor = connection.cursor()
    cursor.execute(query)
    row_count = cursor.fetchone()[0]
    cursor.close()
    min_row_count = expected_row_count - (expected_row_count * deviation_percent / 100)
    max_row_count = expected_row_count + (expected_row_count * deviation_percent / 100)
    return min_row_count <= row_count <= max_row_count

# Main function to perform data quality checks
def run_data_quality_check(data_config, expected_date):
    passed_tables = []
    failed_tables = []

    connection = conn

    for config in data_config:
        try:
            table_name = config["table_name"]
            timestamp_columns = config["timestamp_columns"]
            pk_column = config["pk_column"]
            metric_columns = config["metric_columns"]
            expected_row_group = config["expect_row_group"]
            expected_row_count = config["expect_row"]
            deviation_percent = config["expect_row_deviation_percent"]
        except:
            expected_row_group = None
            pass

        table_status = {
            "table_name": table_name,
            "column_check": [],
            "row_count_check": False
        }

        # 1. Check if table exists
        if table_exists(connection, table_name):
            table_status["column_check"].append("Table exists")
        else:
            table_status["column_check"].append("Table does not exist")
            failed_tables.append(table_status)
            continue

        # 2. Check if all specified columns exist
        all_columns_exist = True
        if timestamp_columns and not columns_exist(connection, table_name, timestamp_columns):
            table_status["column_check"].append("Timestamp columns do not exist")
            all_columns_exist = False
        if pk_column and not columns_exist(connection, table_name, pk_column.split("|")):
            table_status["column_check"].append("PK columns do not exist")
            all_columns_exist = False
        if metric_columns and not columns_exist(connection, table_name, metric_columns):
            table_status["column_check"].append("Metric columns do not exist")
            all_columns_exist = False

        # 3. Row count check
        if table_name == "STORE_SALES":
            if row_count_valid_SS(connection, table_name, expected_date, expected_row_group, expected_row_count, deviation_percent):
                table_status["row_count_check"] = True
            else:
                table_status["column_check"].append("Row count does not meet expectation")
                table_status["row_count_check"] = False
        else:
            print(expected_row_group)
            if expected_row_group != None:
                if row_count_valid(connection, table_name, expected_date, expected_row_group, expected_row_count, deviation_percent):
                    table_status["row_count_check"] = True
                else:
                    table_status["column_check"].append("Row count does not meet expectation")
                    all_columns_exist = False
            else:
                 table_status["row_count_check"] = True


        # Final pass/fail decision
        if all_columns_exist and table_status["row_count_check"]:
            passed_tables.append(table_status)
        else:
            failed_tables.append(table_status)

    connection.close()

    return passed_tables, failed_tables

def main(json_file):

    # JSON Data configuration
    data_config = load_json_config(json_file)
    expected_date = "2001-10-27"

    # Run the check and print the results
    passed, failed = run_data_quality_check(data_config, expected_date)

    print("Passed Tables:")
    for table in passed:
        print(table["table_name"])

    print("\nFailed Tables:")
    for table in failed:
        print(f"{table['table_name']} - Issues: {', '.join(table['column_check'])}")

main('tables_config.json')