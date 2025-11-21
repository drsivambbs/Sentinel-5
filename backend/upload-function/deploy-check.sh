#!/bin/bash

echo "=== Pre-deployment Checks ==="

# 1. Check required APIs
echo "1. Checking required APIs..."
gcloud services list --enabled --filter="name:storage-api.googleapis.com OR name:cloudfunctions.googleapis.com OR name:run.googleapis.com" --format="value(name)"

echo "Enabling required APIs..."
gcloud services enable storage-api.googleapis.com
gcloud services enable cloudfunctions.googleapis.com  
gcloud services enable run.googleapis.com

# 2. Check IAM permissions
echo "2. Checking IAM permissions..."
gcloud projects get-iam-policy $(gcloud config get-value project) --flatten="bindings[].members" --format="table(bindings.role)" --filter="bindings.members:$(gcloud config get-value account)"

# 3. Set Mumbai region
echo "3. Setting Mumbai region..."
gcloud config set functions/region asia-south1
gcloud config set run/region asia-south1

echo "=== Checks Complete ==="