#!/bin/bash

# Dashboard API Cloud Run Deployment Script

PROJECT_ID="sentinel-h-5"
SERVICE_NAME="sentinel-dashboard-api"
REGION="asia-south1"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

echo "🚀 Deploying Dashboard API to Cloud Run..."

# Build and push Docker image
echo "📦 Building Docker image..."
gcloud builds submit --tag ${IMAGE_NAME} --project ${PROJECT_ID}

# Deploy to Cloud Run
echo "🌐 Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
  --image ${IMAGE_NAME} \
  --platform managed \
  --region ${REGION} \
  --allow-unauthenticated \
  --set-env-vars="PROJECT_ID=${PROJECT_ID},DATASET_ID=sentinel_h_5,TABLE_ID=patient_records" \
  --memory=512Mi \
  --cpu=1 \
  --timeout=300 \
  --project ${PROJECT_ID}

# Get service URL
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} --region=${REGION} --project=${PROJECT_ID} --format="value(status.url)")

echo "✅ Deployment complete!"
echo "📍 Service URL: ${SERVICE_URL}"
echo "🔗 API Endpoint: ${SERVICE_URL}/api/dashboard/stats"

# Update config.json with the new URL
echo "📝 Update your config.json with:"
echo "\"dashboard_api\": \"${SERVICE_URL}/api/dashboard/stats\""