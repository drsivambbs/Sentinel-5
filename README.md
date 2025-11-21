# Sentinel-5

Automated patient data processing system with CSV upload, BigQuery sync, and geocoding capabilities.

## System Overview

Sentinel-5 provides a complete pipeline for processing patient records:
1. **CSV Upload** - Web interface for file uploads
2. **BigQuery Sync** - Automatic data ingestion and deduplication
3. **Geocoding** - Automated address-to-coordinates conversion

## Architecture

```
CSV Upload → Cloud Storage → BigQuery Sync → Geocoding → Final Dataset
```

### Components

#### 1. Upload Function (`backend/upload-function/`)
- **Technology**: Node.js Cloud Function
- **Trigger**: HTTP POST with CSV file
- **Function**: Validates and uploads CSV to Cloud Storage
- **Location**: `gs://sentinel-cases-bucket/upload/`

#### 2. BigQuery Sync (`backend/bigquery-sync/`)
- **Technology**: Python Cloud Function
- **Trigger**: Cloud Storage file creation
- **Function**: 
  - Processes CSV files from storage
  - Handles data type conversion and validation
  - Prevents duplicate records using `unique_id`
  - Inserts data into BigQuery table
- **Target**: `sentinel-h-5.sentinel_h_5.patient_records`

#### 3. Geocoding Service (`backend/geocoding-function/`)
- **Technology**: Python Cloud Function
- **Trigger**: Cloud Scheduler (every 15 minutes)
- **Function**:
  - Processes 100 records per batch
  - Geocodes addresses using Google Maps API
  - Updates latitude/longitude in BigQuery
  - Uses persistent cache to reduce API costs
- **Location**: Mumbai (asia-south1)

## Data Schema

Based on `Reference/excel.json` - 79 columns including:
- **Identity**: `patient_id`, `unique_id`, `site_code`
- **Demographics**: `patient_name`, `pat_age`, `pat_sex`
- **Location**: `statename`, `districtname`, `villagename`, `pat_street`, `pat_house`, `pat_pincode`
- **Coordinates**: `latitude`, `longitude` (auto-populated by geocoding)
- **Medical**: 30+ symptom fields (`sym_*`), vital signs (`vit_*`)

## Deployment

### Prerequisites
- Google Cloud Project: `sentinel-h-5`
- APIs enabled: Cloud Functions, BigQuery, Secret Manager, Cloud Scheduler
- Google Maps API key stored in Secret Manager as `google_map_api_key`

### Deploy Upload Function
```bash
cd backend/upload-function
./deploy.sh
```

### Deploy BigQuery Sync
```bash
cd backend/bigquery-sync
./deploy.sh
```

### Deploy Geocoding Service
```bash
cd backend/geocoding-function
gcloud functions deploy geocode-addresses --gen2 --runtime=python311 --region=asia-south1 --source=. --entry-point=geocode_addresses --trigger-http --allow-unauthenticated --set-env-vars="PROJECT_ID=sentinel-h-5,DATASET_ID=sentinel_h_5,TABLE_ID=patient_records,GEOCODING_SECRET_NAME=google_map_api_key,CACHE_TABLE=sentinel_h_5.geocode_cache" --memory=512MB --timeout=540s
```

### Setup Scheduler
```bash
gcloud scheduler jobs create http geocoding-scheduler --schedule="*/15 * * * *" --uri="https://asia-south1-sentinel-h-5.cloudfunctions.net/geocode-addresses" --http-method=POST --location=asia-south1 --time-zone="Asia/Kolkata" --description="Geocode 100 records every 15 minutes"
```

## Usage

### Upload CSV
1. Access web interface at deployed upload function URL
2. Select CSV file with patient records
3. Submit - file automatically processed

### Monitor Progress
- **BigQuery Console**: View `sentinel_h_5.patient_records` table
- **Cloud Functions Logs**: Monitor processing status
- **Geocoding Progress**: Check `latitude`/`longitude` population

### Query Examples

**Check geocoding progress**:
```sql
SELECT 
  COUNT(*) as total_records,
  COUNT(latitude) as geocoded_records,
  COUNT(*) - COUNT(latitude) as remaining
FROM `sentinel-h-5.sentinel_h_5.patient_records`;
```

**View geocoded records**:
```sql
SELECT unique_id, pat_street, villagename, districtname, latitude, longitude
FROM `sentinel-h-5.sentinel_h_5.patient_records`
WHERE latitude IS NOT NULL
LIMIT 10;
```

## Performance

- **Upload**: Immediate processing
- **BigQuery Sync**: ~1-2 minutes per CSV
- **Geocoding**: 100 records every 15 minutes
- **Daily Capacity**: 9,600 geocoded records
- **API Optimization**: Persistent cache prevents duplicate geocoding

## Security Features

- **API Keys**: Stored in Google Secret Manager
- **Access Control**: Cloud IAM permissions
- **Data Validation**: Input sanitization and type checking
- **Error Handling**: Comprehensive logging and recovery
- **Rate Limiting**: API call throttling to prevent quota exhaustion

## Monitoring

- **Cloud Functions**: Execution logs and metrics
- **BigQuery**: Query history and table statistics
- **Cloud Scheduler**: Job execution status
- **Secret Manager**: API key access logs

## Troubleshooting

**Streaming Buffer Issues**:
- Geocoding pauses when BigQuery has streaming buffer
- Automatically resumes when buffer clears (2-6 hours)
- Check with: `UPDATE table SET latitude = latitude WHERE unique_id = 'test'`

**Failed Geocoding**:
- Invalid addresses logged but processing continues
- Check Cloud Functions logs for specific errors
- Cached failed attempts prevent repeated API calls

**Duplicate Records**:
- System prevents duplicates using `unique_id`
- Existing records skipped during CSV processing

## Environment Variables

```bash
PROJECT_ID=sentinel-h-5
DATASET_ID=sentinel_h_5
TABLE_ID=patient_records
BUCKET_NAME=sentinel-cases-bucket
FOLDER_NAME=upload
GEOCODING_SECRET_NAME=google_map_api_key
CACHE_TABLE=sentinel_h_5.geocode_cache
```

## License

Internal use only - Sentinel-5 Patient Data Processing System