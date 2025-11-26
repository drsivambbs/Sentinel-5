#!/bin/bash

# Deploy Sentinel-5 Cluster Function to Cloud Run
set -e

PROJECT_ID="sentinel-h-5"
SERVICE_NAME="cluster-function"
REGION="asia-south1"

echo "Deploying cluster function..."

gcloud run deploy $SERVICE_NAME \
  --source . \
  --platform managed \
  --region $REGION \
  --project $PROJECT_ID \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 900 \
  --concurrency 1 \
  --max-instances 3 \
  --set-env-vars="GCP_PROJECT_ID=$PROJECT_ID,DATASET_ID=sentinel_h_5"

echo "Deployment complete!"
echo "Service URL: https://$SERVICE_NAME-$(gcloud config get-value project | tr ':' '-' | tr '.' '-').${REGION}.run.app"