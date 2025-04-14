import pandas as pd
import pyarrow as pa

# Function to retrieve the schema of a BigQuery table
def get_bq_schema(client_bq, dataset_id, bigquery_table):
    # Get the table schema from BigQuery
    bigquery_table = f"{dataset_id}.{bigquery_table}"
    table = client_bq.get_table(bigquery_table)
    return table.schema

# Function to dynamically adjust DataFrame column types based on BigQuery schema
def adjust_dataframe_types(df, bq_schema):
    arrow_fields = []  # Store schema for PyArrow

    for field in bq_schema:
        column_name = field.name
        column_type = field.field_type

        # Ensure the column exists in the DataFrame before modifying
        if column_name in df.columns:
            if column_type == "STRING":
                df[column_name] = df[column_name].astype(str)
                arrow_fields.append(pa.field(column_name, pa.string()))
            elif column_type == "INTEGER":
                df[column_name] = pd.to_numeric(df[column_name], errors="coerce").fillna(0).astype("Int64")
                arrow_fields.append(pa.field(column_name, pa.int64()))
            elif column_type == "FLOAT":
                df[column_name] = pd.to_numeric(df[column_name], errors="coerce").astype(float)
                arrow_fields.append(pa.field(column_name, pa.float64()))
            elif column_type == "BOOLEAN":
                df[column_name] = df[column_name].astype(bool)
                arrow_fields.append(pa.field(column_name, pa.bool_()))
            elif column_type == "DATE":
                df[column_name] = pd.to_datetime(df[column_name], errors="coerce").dt.date
                arrow_fields.append(pa.field(column_name, pa.date32()))
            elif column_type == "TIMESTAMP":
                df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
                arrow_fields.append(pa.field(column_name, pa.timestamp("ms")))
            # Add additional types as needed (e.g., NUMERIC, BYTES, ARRAY)

    # Create PyArrow schema
    ref_schema = pa.schema(arrow_fields)

    return df, ref_schema  # Return both the adjusted DataFrame and the PyArrow schema