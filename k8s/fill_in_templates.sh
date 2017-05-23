#!/bin/bash

# Fills in deployment templates.  Depending on when your version of python 2.7
# was released, this code might not work.  In particular, prior to the fix of
#  http://bugs.python.org/issue17078
# string.safe_substitute was broken for the use case in mlabconfig. Travis has a
# recent-enough version, and if you are trying to run th script locally and you
# end up with strings like "{{IMAGE" in your file without a closing "}}", then
# your version of python does not have the bugfix.

USAGE="$0 <pattern> <storage_size>"
PATTERN=${1:?Please provide a pattern for mlabconfig: $USAGE}
GIGABYTES=${2:?Please give an integer number of gigabytes: $USAGE}

# claims/ is also referenced in deploy.sh
mkdir -p claims

# deployment/ is also referenced in travis/build_and_deploy_container.sh
mkdir -p deployment

./operator/plsync/mlabconfig.py \
    --format=scraper_kubernetes \
    --template_input=k8s/deploy.yml \
    --template_output=deployment/deploy-{{site_safe}}-{{node_safe}}-{{experiment_safe}}-{{rsync_module_safe}}.yml \
    --select="${PATTERN}"

sed -e "s/{{GIGABYTES}}/${GIGABYTES}/" k8s/claim.yml > k8s/claim_template.yml
./operator/plsync/mlabconfig.py \
    --format=scraper_kubernetes \
    --template_input=k8s/claim_template.yml \
    --template_output=claims/claim-{{site_safe}}-{{node_safe}}-{{experiment_safe}}-{{rsync_module_safe}}.yml \
    --select="${PATTERN}"
