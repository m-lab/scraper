[![Build Status](https://travis-ci.org/m-lab/scraper.svg?branch=master)](https://travis-ci.org/m-lab/scraper)
[![Coverage Status](https://coveralls.io/repos/github/m-lab/scraper/badge.svg?branch=master)](https://coveralls.io/github/m-lab/scraper?branch=master)

# Scraper
Scrape experiment data off of MLab nodes and upload it to the ETL pipeline.

# Requirements

The libraries required to run the project are in `requirements.txt`, the
libraries required to run the tests and the commit hooks are in
`test-requirements.txt`. To develop code in this repo, you will want both, so
you should probably type `pip install -r requirements.txt` and `pip install -r
test-requirements.txt`

# Building and running

To build and push the image to GCR and deploy to production, type
```bash
./deploy.sh production
```

To build and push the image to GCR and deploy to staging, type
```bash
./deploy.sh staging
```

The rest of this doc describes how to run the image locally or to set up a cluster from scratch.

To run the image locally, try:
```bash
sudo docker build . -t scraper && \
  sudo docker run -it -p 9090:9090 \
    -e RSYNC_MODULE=ndt \
    -e RSYNC_HOST=ndt.iupui.mlab1.yyz01.measurement-lab.org \
    scraper
```

If you would like to run things on your own cluster, you will need your own
cluster!  I created the cluster in staging with the following command lines:
```bash
gcloud container \
  --project "mlab-oti" clusters create "scraper-cluster" \
  --zone "us-central1-a" \
  --machine-type "n1-standard-1" \
  --image-type "GCI" \
  --disk-size "40" \
  --scopes "https://www.googleapis.com/auth/cloud-platform","https://www.googleapis.com/auth/spreadsheets" \
  --num-nodes "200" \
  --network "default" \
  --enable-cloud-logging \
  --node-labels=scraper-node=true \
  --no-enable-cloud-monitoring

gcloud --project=mlab-sandbox container node-pools create prometheus-pool \
  --cluster=scraper-cluster \
  --num-nodes=2 \
  --node-labels=prometheus-node=true \
  --machine-type=n1-standard-8
```
