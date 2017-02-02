#!/bin/bash

gcloud container \
  --project "mlab-staging" clusters create "scraper-staging-cluster" \
  --zone "us-central1-a" \
  --machine-type "n1-highmem-16" \
  --image-type "GCI" \
  --disk-size "400" \
  --scopes "https://www.googleapis.com/auth/userinfo.email","https://www.googleapis.com/auth/compute","https://www.googleapis.com/auth/devstorage.read_write","https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/servicecontrol","https://www.googleapis.com/auth/service.management.readonly","https://www.googleapis.com/auth/trace.append","https://www.googleapis.com/auth/spreadsheets" \
  --num-nodes "3" \
  --network "default" \
  --enable-cloud-logging \
  --no-enable-cloud-monitoring

