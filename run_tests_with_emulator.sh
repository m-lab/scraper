#!/bin/bash

# This file should only be run *inside* a container. It starts up a datastore
# emulator, runs the local tests, and never shuts down the datastore.  If it
# exits successfully, then all the unit tests ran successfully.

set -e
set -x

source /root/google-cloud-sdk/path.bash.inc
gcloud config set project mlab-sandbox
gcloud beta emulators datastore start --consistency=1.0 --no-store-on-disk &
mkdir -p /tmp/iupui_ndt
cat > /tmp/rsyncd.conf <<EOF
pid file=/tmp/rsyncd.pid
port=7999
[iupui_ndt]
    comment = Data from iupui_ndt : See http://www.measurementlab.net
    path = /tmp/iupui_ndt
    list = yes
    read only = yes
    transfer logging = no
    ignore errors = no
    ignore nonreadable = yes
EOF
rsync --daemon --address=127.0.0.1 --config=/tmp/rsyncd.conf
sleep 5  # Lose the race condition with the datastore and rsync starts
$(gcloud beta emulators datastore env-init)
env
./git-hooks/python-pre-commit
rm *.pyc
