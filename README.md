[![Build Status](https://travis-ci.org/m-lab/signal-searcher.svg?branch=master)](https://travis-ci.org/m-lab/signal-searcher)
[![Coverage Status](https://coveralls.io/repos/github/m-lab/scraper/badge.svg?branch=master)](https://coveralls.io/github/m-lab/scraper?branch=master)

# Scraper
Scrape experiment data off of MLab nodes and upload it to the ETL pipeline.

# Tests
This repo is fully integrated with Travis, but with one wrinkle.  The
end-to-end test can't (currently) be run on Travis.  So the end-to-end test has
been included as a requirement of the pre-commit git hook.  This means that you
can only develop scraper code in a GCE instance.

# Requirements

The libraries required to run the project are in `requirements.txt`, the libraries required to run the tests and the commit hooks are in `test-requirements.txt`.

# Building and running

Try: 
```bash
sudo docker build . -t scraper && sudo docker run -it -p 9090:9090 -e RSYNC_MODULE=ndt -e RSYNC_HOST=ndt.iupui.mlab1.yyz01.measurement-lab.org scraper
```

If you would like to run things on your own cluster, then you'll need to use scripts from the operator repository to fill in `deploy.yml`. You will also, of course, need your own cluster!  I created the cluster in staging with the following command line:
```bash
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
```
