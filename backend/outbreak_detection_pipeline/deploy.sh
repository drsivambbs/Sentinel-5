#!/bin/bash

# Outbreak Detection Pipeline Step 1 - Cloud Run Deployment Script
# Deploy to Mumbai (asia-south1)

set -e

# Configuration
PROJECT_ID="sentinel-h-5"
SERVICE_NAME="outbreak-detection-pipeline-step1"
REGION="asia-south1"
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

echo "=========================================="
echo "DEPLOYING OUTBREAK DETECTION PIPELINE STEP 1"
echo "=========================================="
echo "Project: ${PROJECT_ID}"
echo "Service: ${SERVICE_NAME}"
echo "Region: ${REGION}"
echo "Image: ${IMAGE_NAME}"
echo "=========================================="

# Build and push Docker image
echo "Building Docker image..."
docker build -t ${IMAGE_NAME} .

echo "Pushing image to Container Registry..."
docker push ${IMAGE_NAME}

# Deploy to Cloud Run
echo "Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
    --image=${IMAGE_NAME} \
    --platform=managed \
    --region=${REGION} \
    --allow-unauthenticated \
    --memory=2Gi \
    --cpu=2 \
    --timeout=3600 \
    --concurrency=1 \
    --max-instances=10 \
    --set-env-vars="GCP_PROJECT=${PROJECT_ID}" \
    --set-env-vars="DATASET_ID=sentinel_h_5" \
    --set-env-vars="DAYS_BACK=7" \
    --set-env-vars="GEOCODING_THRESHOLD=90" \
    --set-env-vars="CLUSTER_TIME_WINDOW=7" \
    --set-env-vars="MIN_CLUSTER_SIZE=2" \
    --set-env-vars="GIS_EPS_METERS=350" \
    --project=${PROJECT_ID}

# Get service URL
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} --region=${REGION} --format="value(status.url)")

echo "=========================================="
echo "DEPLOYMENT COMPLETE!"
echo "=========================================="
echo "Service URL: ${SERVICE_URL}"
echo "Health Check: ${SERVICE_URL}/health"
echo "Pipeline Endpoint: ${SERVICE_URL}/outbreak-detection-step1"
echo ""
echo "Test with:"
echo "curl -X POST ${SERVICE_URL}/outbreak-detection-step1"
echo "=========================================="