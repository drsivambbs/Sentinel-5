#!/bin/bash

# Pre-deployment checks
./deploy-check.sh

# Create GCS bucket in Mumbai
gsutil mb -l asia-south1 gs://sentinel-cases-bucket

# Build and deploy to Cloud Run (Mumbai)
gcloud builds submit --tag gcr.io/$(gcloud config get-value project)/sentinel-upload

gcloud run deploy sentinel-upload-service \
  --image gcr.io/$(gcloud config get-value project)/sentinel-upload \
  --platform managed \
  --region asia-south1 \
  --allow-unauthenticated \
  --set-env-vars PROJECT_NAME=sentinel-5-project,BUCKET_NAME=sentinel-cases-bucket,FOLDER_NAME=upload

echo "Deployment complete in Mumbai region!"