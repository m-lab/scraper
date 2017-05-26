#!/bin/bash

# A script that builds and deploys scraper containers and their associated
# storage. Meant to be called from the root directory of the repo or by Travis
# from wherever travis calls things. In the Travis case, it is expected that $2
# will equal travis.

USAGE="$0 [production|staging|arbitrary-string-sandbox] travis?"
if [[ -n "$2" ]] && [[ "$2" != travis ]]; then
  echo The second argument can only be the word travis or nothing at all.
  echo $USAGE
  exit 1
fi

set -e
set -x

if [[ $2 == travis ]]; then
  cd $TRAVIS_BUILD_DIR
  GIT_COMMIT=${TRAVIS_COMMIT}
else
  GIT_COMMIT=$(git log -n 1 | head -n 1 | awk '{print $2}')
fi

source "${HOME}/google-cloud-sdk/path.bash.inc"

if [[ -e deployment ]] || [[ -e claims ]]; then
  echo "You must remove existing deployment/ and claims/ directories"
  exit 1
fi
mkdir deployment
mkdir claims

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

if [[ "$1" == production ]]
then
  # We need more quota to support these numbers
  # TODO(dev) get 100+ T of quota
  # No mlab4s in prod until our quota goes up.
  fill_in_templates '.*\.mlab[123]\.[a-z]{3}\d\d\..*' 10 claims deployment
  fill_in_templates '.*ndt.*\.mlab[123]\.[a-z]{3}\d\d\..*' 110 claims deployment
  PROJECT=mlab-oti
  BUCKET=scraper-mlab-oti
  DATASTORE_NAMESPACE=scraper
  CLUSTER=scraper-cluster
  ZONE=us-central1-a
elif [[ "$1" == staging ]]
then
  # These are the machines we deploy staging images to.
  # no mlab4s until more bugs are worked out
  fill_in_templates 'mlab4' 11 claims deployment
  fill_in_templates 'ndt.*mlab4' 110 claims deployment
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
  KEY_FILE=/tmp/staging-secret-key.json
  PROJECT=mlab-staging
  BUCKET=scraper-mlab-staging
  DATASTORE_NAMESPACE=scraper
  CLUSTER=scraper-cluster
  ZONE=us-central1-a
else
  echo "Bad argument to $0"
  exit 1
fi

if [[ $2 == travis ]]; then
  # On Travis, use a service account
  gcloud auth activate-service-account --key-file ${KEY_FILE}
fi

# Set up the deploy.yml files
./travis/substitute_values.sh deployment \
    IMAGE_URL gcr.io/${PROJECT}/github-m-lab-scraper:${GIT_COMMIT} \
    GCS_BUCKET ${BUCKET} \
    NAMESPACE ${DATASTORE_NAMESPACE} \
    GITHUB_COMMIT http://github.com/m-lab/scraper/tree/${GIT_COMMIT}

# Build the container and save it to GCR
./travis/build_and_push_container.sh \
    gcr.io/${PROJECT}/github-m-lab-scraper:${GIT_COMMIT} ${PROJECT}

# Make sure kubectl is associated with the right cluster
gcloud --project=${PROJECT} container clusters get-credentials ${CLUSTER} --zone=${ZONE}

# Define terms for later use in our claims and deployments
kubectl apply -f k8s/namespace.yml
kubectl apply -f k8s/storage-class.yml

# Define all our claims
CLAIMSOUT=$(mktemp claims.XXXXXX)
kubectl apply -f claims/ > ${CLAIMSOUT} || (cat ${CLAIMSOUT} && exit 1)
echo Applied $(wc -l ${CLAIMSOUT} | awk '{print $1}') claims

# Define all our deployments
DEPLOYOUT=$(mktemp deployments.XXXXXX)
kubectl apply -f deployment/ > ${DEPLOYOUT} || (cat ${DEPLOYOUT} && exit 1)
echo Applied $(wc -l ${DEPLOYOUT} | awk '{print $1}') deployments

# Output debug info
echo kubectl returned success from "'$0 $@'" for all operations.
echo Suppressed output is appended below to aid future debugging:
echo Output of successful "'kubectl apply -f claims/'":
cat ${CLAIMSOUT}
rm ${CLAIMSOUT}

echo Output of successful "'kubectl apply -f deployment/'":
cat ${DEPLOYOUT}
rm ${DEPLOYOUT}
