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
        return
    
    # Initialize clients
    storage_client = storage.Client()
    bq_client = bigquery.Client()
    
    # Download CSV from GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    csv_content = blob.download_as_text()
    
    # Read CSV into DataFrame
    df = pd.read_csv(io.StringIO(csv_content))
    
    # Configure BigQuery table reference
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Insert records one by one with proper deduplication
    for _, row in df.iterrows():
        unique_id = row.get('unique_id', '')
        
        # Check if record exists
        check_query = f"SELECT COUNT(*) as count FROM `{table_id}` WHERE unique_id = @unique_id"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("unique_id", "STRING", unique_id)
            ]
        )
        
        result = bq_client.query(check_query, job_config=job_config).result()
        exists = list(result)[0].count > 0
        
        if not exists:
            # Insert new record directly, handling NaN values and data types
            row_dict = dict(row)
            
            # Replace NaN values with None and convert data types
            for key, value in row_dict.items():
                if pd.isna(value):
                    row_dict[key] = None
                elif key == 'patient_entry_date' and value:
                    # Convert MM-DD-YYYY to YYYY-MM-DD
                    try:
                        date_parts = str(value).split('-')
                        if len(date_parts) == 3:
                            row_dict[key] = f"{date_parts[2]}-{date_parts[0].zfill(2)}-{date_parts[1].zfill(2)}"
                    except:
                        row_dict[key] = None
                elif key.startswith('sym_') and value in ['Y', 'N']:
                    # Convert Y/N to boolean
                    row_dict[key] = value == 'Y'
                elif key.startswith('sym_') and value in ['true', 'false']:
                    # Convert string boolean to actual boolean
                    row_dict[key] = value == 'true'
            
            rows_to_insert = [row_dict]
            errors = bq_client.insert_rows_json(table_id, rows_to_insert)
            
            if errors:
                print(f"Error inserting row: {errors}")
            else:
                print(f"Inserted record with unique_id: {unique_id}")
    
    print(f"Processed {file_name} - {len(df)} rows")