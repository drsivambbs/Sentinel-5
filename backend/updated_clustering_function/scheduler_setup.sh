#!/bin/bash

# Setup Cloud Scheduler for Updated Clustering Function
echo "⏰ Setting up Cloud Scheduler..."

# Delete existing scheduler if exists
gcloud scheduler jobs delete clustering-scheduler-v2 --location=asia-south1 --quiet 2>/dev/null || true

# Create new scheduler
gcloud scheduler jobs create http clustering-scheduler-v2 \
  --schedule="*/30 * * * *" \
  --uri="https://asia-south1-sentinel-h-5.cloudfunctions.net/cluster-analysis-v2" \
  --http-method=POST \
  --location=asia-south1 \
  --time-zone="Asia/Kolkata" \
  --description="Updated clustering analysis every 30 minutes"

echo "✅ Scheduler setup completed!"
echo "🔄 Clustering will run every 30 minutes"