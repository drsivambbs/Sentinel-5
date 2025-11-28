#!/bin/bash

gcloud functions deploy export-data \
  --gen2 \
  --runtime=python311 \
  --region=asia-south1 \
  --source=. \
  --entry-point=export_data \
  --trigger-http \
  --allow-unauthenticated \
  --memory=512MB \
  --timeout=300s \
  --set-env-vars="PROJECT_ID=sentinel-h-5"