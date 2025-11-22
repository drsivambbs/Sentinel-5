#!/bin/bash

# Setup BigQuery Scheduled Query for Cluster Merging

# Create the scheduled query
bq query \
  --use_legacy_sql=false \
  --location=asia-south1 \
  --schedule='0 2 * * *' \
  --display_name='Daily Cluster Merge' \
  --replace=true \
  "$(cat cluster_merge.sql)"

echo "Scheduled query created successfully"
echo "Runs daily at 2 AM Asia/Kolkata time"
echo "View in BigQuery Console: Scheduled Queries"