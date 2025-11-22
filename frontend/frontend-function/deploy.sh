#!/bin/bash

gcloud functions deploy fetch-pending \
  --gen2 \
  --runtime=nodejs20 \
  --region=asia-south1 \
  --source=. \
  --entry-point=fetch_pending \
  --trigger-http \
  --allow-unauthenticated \
  --memory=256MB \
  --timeout=60s