#!/bin/bash

# Deploy Smart Clustering Engine to Cloud Run

PROJECT_ID="sentinel-h-5"
SERVICE_NAME="smart-cluster-engine"
REGION="asia-south1"

echo "Deploying Smart Clustering Engine..."

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
  --max-instances 10 \
  --set-env-vars="GCP_PROJECT_ID=$PROJECT_ID,DATASET_ID=sentinel_h_5"

echo "Deployment complete!"
echo "Service URL: https://$SERVICE_NAME-196547645490.$REGION.run.app"