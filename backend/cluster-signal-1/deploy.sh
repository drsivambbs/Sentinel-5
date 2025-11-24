#!/bin/bash

# Deploy cluster-signal-1 service to Cloud Run
gcloud run deploy cluster-signal-1 \
    --source . \
    --platform managed \
    --region asia-south1 \
    --allow-unauthenticated \
    --memory 1Gi \
    --cpu 1 \
    --timeout 3600 \
    --set-env-vars="PROJECT_ID=sentinel-h-5,DATASET_ID=sentinel_h_5"