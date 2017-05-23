[![Build Status](https://travis-ci.org/m-lab/scraper.svg?branch=master)](https://travis-ci.org/m-lab/scraper)
[![Coverage Status](https://coveralls.io/repos/github/m-lab/scraper/badge.svg?branch=master)](https://coveralls.io/github/m-lab/scraper?branch=master)

# Scraper
Scrape experiment data off of MLab nodes and upload it to the ETL pipeline.

# Requirements

The libraries required to run the project are in `requirements.txt`, the
libraries required to run the tests and the commit hooks are in
`test-requirements.txt`.

# Building and running

Try:
```bash
sudo docker build . -t scraper && \
  sudo docker run -it -p 9090:9090 \
    -e RSYNC_MODULE=ndt \
    -e RSYNC_HOST=ndt.iupui.mlab1.yyz01.measurement-lab.org \
    scraper
```

If you would like to run things on your own cluster, then you'll need to use
scripts from the operator repository to fill in `deploy.yml`. You will also, of
course, need your own cluster!  I created the cluster in staging with the
following command line:
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

The cluster, once it is created (and this step need only be done once per
cluster lifetime) needs to have the namespace `scraper` created within it.  That
is accomplished with:
```bash
  kubectl create -f namespace.yml
```
If you would like to be sure you are creating the namespace in the right
cluster, `kubectl config get-contexts` is the command to use.

Once you have a cluster, you will need to deploy to the cluster. To do this, you
need to have a docker image in hand.  Every push to master generates [a new
docker image stored on Google
cloud](https://pantheon.corp.google.com/gcr/images/mlab-sandbox/GLOBAL/github-m-lab-scraper?project=mlab-sandbox),
so choose an image there and put its URL in the `image:` entry in
[deploy.yml](deploy.yml).  The resulting line should look something like:
```yaml
   image: gcr.io/mlab-sandbox/github-m-lab-scraper:09ba5fe4b7ca3f880114c23eafa255598cbb70f0
```

Next, make sure the [operator/](operator) submodule is up to
date and then generate a deployment with
[operator/plsync/mlabconfig.py](//github.com/m-lab/operator/plsync/mlabconfig.py)
as follows:
```bash
mkdir -p deployment  # -p prevents failure when the dir already exists
python ./operator/plsync/mlabconfig.py \
  --format=scraper_kubernetes \
  --select=='.*[0-9][0-9].measurement-lab.org.*' \
  --template_input=deploy.yml \
  --template_output=deployment/{{site}}-{{node}}-{{experiment}}-{{rsync_module}}.yml
```

The above commandline will generate a config for every rsync module on every
experiment on every machine in the fleet.  To select a different subset of
machines, change the `--select=REGEXP` argument. For example, to deploy to all
the `0t` machines, add the argument `--select='.*[0-9]t.measurement-lab.org.*'`.

```
Canaries:
mlab1.atl06
mlab1.dfw01
mlab1.lhr01
mlab1.nuq02
mlab1.wlg02
mlab1.ham01
```

Finally, once you have a deployment, you should push this deployment to
production like so:
```bash
kubectl create -f deployment/
```
