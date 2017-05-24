#!/bin/bash

# A script that builds and deploys scraper containers and their associated
# storage. Meant to be called from the root directory of the repo with a single
# argument: prod, staging, or a string ending in '-sandbox'
#
# This script should work on Travis and when run locally.

set -e
set -x

source "${HOME}/google-cloud-sdk/path.bash.inc"

if [[ -e deployment ]] || [[ -e claims ]]; then
  echo "You must remove existing deployment/ and claims/ directories"
  exit 1
fi
mkdir deployment
mkdir claims

if [[ -n "${TRAVIS_BUILD_DIR}" ]]; then
  cd $TRAVIS_BUILD_DIR
  GIT_COMMIT=${TRAVIS_COMMIT}
else
  GIT_COMMIT=$(git log -n 1 | head -n 1 | awk '{print $2}')
fi

# Fills in deployment templates.
function fill_in_templates() {
  USAGE="$0 <pattern> <storage_size> <claims_dir> <deploy_dir>"
  PATTERN=${1:?Please provide a pattern for mlabconfig: $USAGE}
  GIGABYTES=${2:?Please give an integer number of gigabytes: $USAGE}
  CLAIMS=${3:?Please give a directory for the claims templates: $USAGE}
  DEPLOY=${4:?Please give a directory for the deployment templates: $USAGE}

  ./operator/plsync/mlabconfig.py \
      --format=scraper_kubernetes \
      --template_input=k8s/deploy_template.yml \
      --template_output=${DEPLOY}/deploy-{{site_safe}}-{{node_safe}}-{{experiment_safe}}-{{rsync_module_safe}}.yml \
      --select="${PATTERN}"

  ./operator/plsync/mlabconfig.py \
      --format=scraper_kubernetes \
      --template_input=k8s/claim_template.yml \
      --template_output=${CLAIMS}/claim-{{site_safe}}-{{node_safe}}-{{experiment_safe}}-{{rsync_module_safe}}.yml \
    --select="${PATTERN}"
  ./travis/substitute_values.sh ${CLAIMS} GIGABYTES ${GIGABYTES}
}

if [[ "$1" == staging ]]
then
  # no mlab4s until more bugs are worked out
  #fill_in_templates 'mlab4' 11
  #fill_in_templates 'ndt.*mlab4' 110
  cat operator/plsync/canary_machines.txt | (
      # Disable -x to prevent build log spam
      set +x
      while read
      do
        fill_in_templates "${REPLY}" 11 claims deployment
        fill_in_templates "ndt.*${REPLY}" 110 claims deployment
      done
      # Re-enable -x to aid debugging
      set -x)
  ./travis/substitute_values.sh deployment \
    IMAGE_URL gcr.io/mlab-staging/github-m-lab-scraper:${GIT_COMMIT} \
    GCS_BUCKET scraper-mlab-staging \
    NAMESPACE scraper \
    GITHUB_COMMIT http://github.com/m-lab/scraper/tree/${GIT_COMMIT}
  ./travis/build_and_push_container.sh \
    gcr.io/mlab-staging/github-m-lab-scraper:${GIT_COMMIT} mlab-staging
  gcloud --project=mlab-staging container clusters get-credentials scraper-cluster --zone=us-central1-a
else
  echo "Bad argument to $0"
  exit 1
fi

#kubectl apply -f k8s/namespace.yml
#kubectl apply -f k8s/storage-class.yml
#kubectl apply -f claims/
#kubectl apply -f deployment/
