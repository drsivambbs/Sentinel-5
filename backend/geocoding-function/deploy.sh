#!/bin/bash

gcloud functions deploy geocode-addresses \
  --gen2 \
  --runtime=python311 \
  --region=asia-south1 \
  --source=. \
  --entry-point=geocode_addresses \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars="PROJECT_ID=sentinel-h-5,DATASET_ID=sentinel_h_5,TABLE_ID=patient_records,GEOCODING_SECRET_NAME=google_map_api_key,CACHE_TABLE=sentinel_h_5.geocode_cache" \
  --memory=512MB \
  --timeout=540s