[![Build Status](https://travis-ci.org/m-lab/scraper.svg?branch=master)](https://travis-ci.org/m-lab/scraper)
[![Coverage Status](https://coveralls.io/repos/github/m-lab/scraper/badge.svg?branch=master)](https://coveralls.io/github/m-lab/scraper?branch=master)

# Scraper
Scrape experiment data off of MLab nodes and upload it to the ETL pipeline.

# Development Requirements

All tests are run inside docker containers, so the big requirement is that you
must be able to build and run docker containers.  The `./pre-commit.sh` script
runs the tests and the `./prepare-commit-msg.sh` script makes a nice description
of the current state of test coverage and code health.  I recommend following
the instructions in the comments of each of those files encouraging you to make
symlinks so that the scripts are run automatically on every checkin.  Those
scripts run tests inside Docker containers, and the containers and tests are (or
should be) exactly the same as the ones that Travis runs, so this local testing
should pass if and only if the Travis CI tests will pass.

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
