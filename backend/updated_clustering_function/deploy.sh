#!/bin/bash

# Deploy Updated Clustering Function
echo "🚀 Deploying Updated Clustering Function..."

gcloud functions deploy cluster-analysis-v2 \
  --gen2 \
  --runtime=python311 \
  --region=asia-south1 \
  --source=. \
  --entry-point=cluster_analysis \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars="PROJECT_ID=sentinel-h-5,DATASET_ID=sentinel_h_5,TABLE_ID=patient_records,TEMP_CLUSTER_TABLE=temp_cluster_table,CLUSTER_SUMMARY_TABLE=cluster_summary_table,TIME_WINDOW=7,MIN_CASES=2" \
  --memory=1GB \
  --timeout=540s \
  --max-instances=10

echo "✅ Deployment completed!"
echo "📋 Function URL will be displayed above"