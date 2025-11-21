#!/bin/bash

# Create Cloud Scheduler job for clustering automation
gcloud scheduler jobs create http clustering-scheduler \
  --schedule="*/30 * * * *" \
  --uri="https://asia-south1-sentinel-h-5.cloudfunctions.net/cluster-analysis" \
  --http-method=POST \
  --location=asia-south1 \
  --time-zone="Asia/Kolkata" \
  --description="Run clustering analysis every 30 minutes" \
  --attempt-deadline=540s \
  --max-retry-attempts=3 \
  --min-backoff=60s \
  --max-backoff=300s

echo "Clustering scheduler created successfully!"
echo "Schedule: Every 30 minutes"
echo "Next run: Check Cloud Console for details"