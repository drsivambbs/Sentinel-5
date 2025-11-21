import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import functions_framework
import io
import os

@functions_framework.cloud_event
def sync_to_bigquery(cloud_event):
    data = cloud_event.data
    bucket_name = data['bucket']
    file_name = data['name']
    
    # Load environment variables
    PROJECT_ID = os.getenv('PROJECT_ID', 'sentinel-h-5')
    DATASET_ID = os.getenv('DATASET_ID', 'sentinel_h_5')
    TABLE_ID = os.getenv('TABLE_ID', 'patient_records')
    BUCKET_NAME = os.getenv('BUCKET_NAME', 'sentinel-cases-bucket')
    FOLDER_NAME = os.getenv('FOLDER_NAME', 'upload')
    
    if bucket_name != BUCKET_NAME or not file_name.startswith(f'{FOLDER_NAME}/') or not file_name.endswith('.csv'):
        print(f"File {file_name} in bucket {bucket_name} ignored")
        return
    
    # Initialize clients
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # Verify table exists
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    try:
        bq_client.get_table(table_id)
    except Exception as e:
        print(f"Table {table_id} does not exist or cannot be accessed: {e}")
        return
    
    # Download CSV from GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    csv_content = blob.download_as_text()
    
    # Read CSV into DataFrame
    df = pd.read_csv(io.StringIO(csv_content))
    
    # Fetch existing unique_ids to avoid duplicates in batch
    unique_ids = df['unique_id'].dropna().astype(str).tolist()
    if not unique_ids:
        print("No unique_id found in the CSV file; nothing to insert.")
        return
    
    # Query to find existing unique_ids already in BigQuery
    query = f"""
        SELECT unique_id FROM `{table_id}`
        WHERE unique_id IN UNNEST(@unique_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("unique_ids", "STRING", unique_ids)
        ]
    )
    existing_ids_result = bq_client.query(query, job_config=job_config).result()
    existing_ids = {row.unique_id for row in existing_ids_result}
    
    # Prepare rows to insert -- skip duplicates
    rows_to_insert = []
    for _, row in df.iterrows():
        unique_id = str(row.get('unique_id', ''))
        if unique_id in existing_ids or unique_id == '':
            continue
        
        row_dict = dict(row)
        
        # Replace NaN values with None and convert data types
        for key, value in row_dict.items():
            if pd.isna(value):
                row_dict[key] = None
            elif key == 'patient_entry_date' and value:
                try:
                    from datetime import datetime
                    # Try multiple date formats
                    date_str = str(value).strip()
                    if date_str:
                        # Try DD-MM-YYYY format first (your data format)
                        try:
                            parsed_date = datetime.strptime(date_str, '%d-%m-%Y')
                            row_dict[key] = parsed_date.strftime('%Y-%m-%d')
                        except ValueError:
                            # Try MM-DD-YYYY format
                            try:
                                parsed_date = datetime.strptime(date_str, '%m-%d-%Y')
                                row_dict[key] = parsed_date.strftime('%Y-%m-%d')
                            except ValueError:
                                # Try YYYY-MM-DD format (already correct)
                                try:
                                    parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
                                    row_dict[key] = date_str
                                except ValueError:
                                    row_dict[key] = None
                    else:
                        row_dict[key] = None
                except:
                    row_dict[key] = None
            elif key.startswith('sym_') and value in ['Y', 'N']:
                row_dict[key] = value == 'Y'
            elif key.startswith('sym_') and value in ['true', 'false']:
                row_dict[key] = value == 'true'
        
        rows_to_insert.append(row_dict)
    
    # Insert rows in batch
    if rows_to_insert:
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            print(f"Errors inserting rows: {errors}")
        else:
            print(f"Inserted {len(rows_to_insert)} new records into {table_id}")
    else:
        print("No new records to insert after deduplication.")
    
    print(f"Processed file {file_name} with total {len(df)} rows")