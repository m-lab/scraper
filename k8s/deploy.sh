#!/bin/bash

# A script that builds and deploys scraper containers and their associated
# storage. Meant to be called from the root directory of the repo with a single
# argument: prod, staging, or a string ending in '-sandbox'

set -e
set -x

source "${HOME}/google-cloud-sdk/path.bash.inc"

ssh-keygen -f ~/.ssh/google_compute_engine -N ""
cd $TRAVIS_BUILD_DIR

if [[ "$1" == staging ]]
then
    ./k8s/fill_in_templates.sh 'mlab4' 11
    ./k8s/fill_in_templates.sh 'ndt.*mlab4' 110
    cat operator/plsync/canary_machines.txt | (
        while read
        do
          ./k8s/fill_in_templates.sh "${REPLY}" 11
          ./k8s/fill_in_templates.sh "ndt.*${REPLY}" 110
        done)
    gcloud auth activate-service-account --key-file /tmp/staging-secret-key.json
    gcloud --project=mlab-staging container clusters get-credentials scraper-cluster --zone=us-central1-a
    kubectl apply -f k8s/namespace.yml
    kubectl apply -f k8s/storage-class.yml
    kubectl apply -f claims/
    ./travis/build_and_deploy_container.sh ${TRAVIS_COMMIT} \
      gcr.io/mlab-staging/github-m-lab-scraper mlab-staging scraper-cluster us-central1-a \
      GCS_BUCKET scraper-mlab-staging \
      NAMESPACE scraper \
      GITHUB_COMMIT http://github.com/${TRAVIS_REPO_SLUG}/tree/${TRAVIS_COMMIT}
fi
