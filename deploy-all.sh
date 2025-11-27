#!/bin/bash

set -e

PROJECT_ID="sentinel-h-5"
REGION="asia-south1"

echo "🚀 Starting Sentinel-5 Full Deployment..."
echo "📍 Project: $PROJECT_ID"
echo "🌏 Region: $REGION"

# Set project
gcloud config set project $PROJECT_ID

echo ""
echo "1️⃣ Deploying Upload Function..."
cd backend/upload-function
chmod +x deploy.sh
./deploy.sh
cd ../..

echo ""
echo "2️⃣ Deploying BigQuery Sync Function..."
cd backend/bigquery-sync
chmod +x deploy.sh
./deploy.sh
cd ../..

echo ""
echo "3️⃣ Deploying Geocoding Function..."
cd backend/geocoding-function
chmod +x deploy.sh
./deploy.sh
cd ../..

echo ""
echo "4️⃣ Setting up Geocoding Scheduler..."
gcloud scheduler jobs create http geocoding-scheduler \
  --schedule="*/15 * * * *" \
  --uri="https://$REGION-$PROJECT_ID.cloudfunctions.net/geocode-addresses" \
  --http-method=POST \
  --location=$REGION \
  --time-zone="Asia/Kolkata" \
  --description="Geocode 100 records every 15 minutes" \
  --project=$PROJECT_ID || echo "Scheduler already exists"

echo ""
echo "5️⃣ Deploying Dashboard API..."
cd backend/dashboard-api
chmod +x deploy.sh
./deploy.sh
cd ../..

echo ""
echo "6️⃣ Deploying Smart Cluster Engine..."
cd backend/smart-cluster-engine
chmod +x deploy.sh
./deploy.sh
cd ../..

echo ""
echo "✅ Deployment Complete!"
echo ""
echo "📋 Service URLs:"
echo "🔗 Upload Service: https://sentinel-upload-service-196547645490.$REGION.run.app"
echo "🔗 Dashboard API: https://sentinel-dashboard-api-196547645490.$REGION.run.app"
echo "🔗 Cluster Engine: https://smart-cluster-engine-196547645490.$REGION.run.app"
echo ""
echo "⚡ Functions:"
echo "🔗 BigQuery Sync: https://$REGION-$PROJECT_ID.cloudfunctions.net/sentinel-bigquery-sync"
echo "🔗 Geocoding: https://$REGION-$PROJECT_ID.cloudfunctions.net/geocode-addresses"
echo ""
echo "⏰ Scheduler: geocoding-scheduler (every 15 minutes)"