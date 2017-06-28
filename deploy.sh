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
  # No mlab4s in prod for now.  They are in staging.
  cat operator/plsync/production_patterns.txt \
    | while read PATTERN
      do
        fill_in_templates "${PATTERN}" 11 claims deployment
        fill_in_templates "ndt.*${PATTERN}" 110 claims deployment
      done
  KEY_FILE=/tmp/mlab-oti.json
  PROJECT=mlab-oti
  BUCKET=scraper-mlab-oti
  DATASTORE_NAMESPACE=scraper
  CLUSTER=scraper-cluster
  ZONE=us-central1-a
elif [[ "$1" == staging ]]
then
  # These are the machines we scrape with our staging instance.
  # Disable -x to prevent build log spam
  set +x
  cat operator/plsync/staging_patterns.txt operator/plsync/canary_machines.txt \
    | while read PATTERN
      do
        fill_in_templates "${PATTERN}" 11 claims deployment
        fill_in_templates "ndt.*${PATTERN}" 110 claims deployment
      done
  set -x
  KEY_FILE=/tmp/mlab-staging.json
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
(kubectl apply -f claims/ \
  | tee ${CLAIMSOUT} \
  | awk 'NR % 100 == 1 {print "Claim", NR, $0}') || (cat ${CLAIMSOUT}; exit 1)
echo Applied $(wc -l ${CLAIMSOUT} | awk '{print $1}') claims

# Define all our deployments
DEPLOYOUT=$(mktemp deployments.XXXXXX)
(kubectl apply -f deployment/ \
  | tee ${DEPLOYOUT} \
  | awk 'NR % 100 == 1 {print "Deployment", NR, $0}') || (cat ${DEPLOYOUT};
                                                          exit 1)
echo Applied $(wc -l ${DEPLOYOUT} | awk '{print $1}') deployments

# Delete all jobs in the scraper namespace that do not have a corresponding
# template file.  Also delete their claims.
CURRENT_DEPLOYMENTS=$(mktemp current_deployments.XXXXXX)
kubectl -n scraper get deploy --no-headers \
  | awk '{print $1}' \
  | sort \
  > ${CURRENT_DEPLOYMENTS}
DESIRED_DEPLOYMENTS=$(mktemp desired_deployments.XXXXXX)
ls deployment/ \
  | sed -e 's/^deploy-//' -e 's/.yml$//' \
  | sort \
  > ${DESIRED_DEPLOYMENTS}
# By default, comm displays three columns of output: stuff that's only in the
# first file (1), stuff that's only in the second (2), and stuff that's in both
# (3).  We use -2 and -3 to suppress the reports of (2) and (3), to get a list
# of the current deployments that are not in the list of desired deployments.
comm -3 -2 ${CURRENT_DEPLOYMENTS} ${DESIRED_DEPLOYMENTS} \
  | while read DEPLOY; do
      kubectl -n scraper delete deploy/${DEPLOY}
      kubectl -n scraper delete persistentvolumeclaim/claim-${DEPLOY}
    done
rm ${CURRENT_DEPLOYMENTS} ${DESIRED_DEPLOYMENTS}

# Output debug info
echo kubectl returned success from "'$0 $@'" for all operations.
echo Suppressed output is appended below to aid future debugging:
echo Output of successful "'kubectl apply -f claims/'":
cat ${CLAIMSOUT}
rm ${CLAIMSOUT}

echo Output of successful "'kubectl apply -f deployment/'":
cat ${DEPLOYOUT}
rm ${DEPLOYOUT}
