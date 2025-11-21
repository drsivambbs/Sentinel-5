#!/bin/bash

# Deploy clustering function
gcloud functions deploy cluster-analysis \
  --gen2 \
  --runtime=python311 \
  --region=asia-south1 \
  --source=. \
  --entry-point=cluster_analysis \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars="PROJECT_ID=sentinel-h-5,DATASET_ID=sentinel_h_5,TABLE_ID=patient_records,TEMP_CLUSTER_TABLE=temp_cluster_table,CLUSTER_SUMMARY_TABLE=cluster_summary_table,TIME_WINDOW=7,MIN_CASES=2,GEOCODING_THRESHOLD=90,DATE_RANGE_LIMIT=15" \
  --memory=1GB \
  --timeout=540s

echo "Clustering function deployed successfully!"
echo "URL: https://asia-south1-sentinel-h-5.cloudfunctions.net/cluster-analysis"