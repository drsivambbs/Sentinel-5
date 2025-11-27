#!/bin/bash

gcloud functions deploy get-hebs-data \
  --gen2 \
  --runtime=python311 \
  --region=asia-south1 \
  --source=. \
  --entry-point=get_hebs_data \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars="PROJECT_ID=sentinel-h-5,DATASET_ID=sentinel_h_5,TABLE_ID=patient_records" \
  --memory=512MB \
  --timeout=540s