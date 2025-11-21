#!/bin/bash

# Run pre-deployment checks
./pre-deploy-check.sh

echo "Deploying to Mumbai (asia-south1)..."

# Deploy Cloud Function
gcloud functions deploy sentinel-bigquery-sync \
  --gen2 \
  --runtime=python311 \
  --region=asia-south1 \
  --source=. \
  --entry-point=sync_to_bigquery \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=sentinel-cases-bucket" \
  --env-vars-file=.env \
  --memory=512MB \
  --timeout=300s \
  --allow-unauthenticated

echo "BigQuery sync function deployed in Mumbai!"