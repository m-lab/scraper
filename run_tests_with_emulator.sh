#!/bin/bash

# This file should only be run *inside* a container. It starts up a datastore
# emulator, runs the local tests, and never shuts down the datastore.  If it
# exits successfully, then all the unit tests ran successfully.

set -e
set -x

./git-hooks/python-pre-commit
