#!/bin/bash

# Fills in deployment templates.  Depending on when your version of python 2.7
# was released, this code might not work.  In particular, prior to the fix of
#  http://bugs.python.org/issue17078
# string.safe_substitute was broken for the use case in mlabconfig. Travis has a
# recent-enough version, and if you are trying to run th script locally and you
# end up with strings like "{{IMAGE" in your file without a closing "}}", then
# your version of python does not have the bugfix.

mkdir -p claims
mkdir -p deployment

USAGE="$0 <pattern> <storage_size>"
PATTERN=${1:?Please provide a pattern for mlabconfig: $USAGE}
GIGABYTES=${2:?Please give an integer number of gigabytes: $USAGE}

./operator/plsync/mlabconfig.py \
    --format=scraper_kubernetes \
    --template_input=deploy.yml \
    --template_output=deployment/deploy-{{site_safe}}-{{node_safe}}-{{experiment_safe}}-{{rsync_module_safe}}.yml \
    --select="${PATTERN}"

sed -e "s/{{GIGABYTES}}/${GIGABYTES}/" claim.yml > claim_template.yml
./operator/plsync/mlabconfig.py \
    --format=scraper_kubernetes \
    --template_input=claim_template.yml \
    --template_output=claims/claim-{{site_safe}}-{{node_safe}}-{{experiment_safe}}-{{rsync_module_safe}}.yml \
    --select="${PATTERN}"
