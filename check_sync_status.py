"""Check BigQuery sync status and diagnose issues"""
from google.cloud import storage
from google.cloud import bigquery
import os

# Configuration
PROJECT_ID = 'sentinel-h-5'
DATASET_ID = 'sentinel_h_5'
TABLE_ID = 'patient_records'
BUCKET_NAME = 'sentinel-cases-bucket'
FOLDER_NAME = 'upload'

print("=== Checking BigQuery Sync Status ===\n")

# Check bucket files
print("1. Checking Cloud Storage bucket...")
try:
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=f'{FOLDER_NAME}/', max_results=5))
    print(f"   ✓ Found {len(blobs)} recent files in {BUCKET_NAME}/{FOLDER_NAME}/")
    for blob in blobs:
        print(f"     - {blob.name} ({blob.size} bytes, {blob.time_created})")
except Exception as e:
    print(f"   ✗ Error accessing bucket: {e}")

# Check BigQuery table
print("\n2. Checking BigQuery table...")
try:
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    table = bq_client.get_table(table_id)
    print(f"   ✓ Table exists: {table_id}")
    print(f"     - Rows: {table.num_rows}")
    print(f"     - Columns: {len(table.schema)}")
    
    # Check recent records
    query = f"SELECT COUNT(*) as count FROM `{table_id}` WHERE DATE(TIMESTAMP(patient_entry_date)) >= '2024-11-01'"
    result = list(bq_client.query(query).result())
    print(f"     - Records since Nov 2024: {result[0].count}")
    
except Exception as e:
    print(f"   ✗ Error accessing BigQuery: {e}")

# Check for column mismatches
print("\n3. Checking for column mismatches...")
try:
    # Get latest file from bucket
    latest_blob = max(blobs, key=lambda b: b.time_created)
    csv_content = latest_blob.download_as_text()
    csv_columns = csv_content.split('\n')[0].split('\t')
    
    # Get BigQuery columns
    bq_columns = [field.name for field in table.schema]
    
    csv_set = set(csv_columns)
    bq_set = set(bq_columns)
    
    extra_in_csv = csv_set - bq_set
    missing_in_csv = bq_set - csv_set
    
    if extra_in_csv:
        print(f"   ⚠ Extra columns in CSV (not in BigQuery): {extra_in_csv}")
    if missing_in_csv:
        print(f"   ⚠ Missing columns in CSV (expected by BigQuery): {missing_in_csv}")
    if not extra_in_csv and not missing_in_csv:
        print(f"   ✓ All columns match!")
        
except Exception as e:
    print(f"   ✗ Error checking columns: {e}")

print("\n=== Diagnosis Complete ===")
print("\nIf files are in bucket but not in BigQuery:")
print("1. Check Cloud Function logs: gcloud functions logs read sentinel-bigquery-sync --region=asia-south1 --limit=50")
print("2. Verify function is deployed: gcloud functions list --region=asia-south1")
print("3. Check function trigger: gcloud functions describe sentinel-bigquery-sync --region=asia-south1")
