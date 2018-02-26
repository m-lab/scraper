#!/bin/bash
#
# Once the container is built, we should run it, which also runs
# git-hooks/python-prepare-commit-msg
#   ln -s ../../prepare-commit-msg.sh .git/hooks/prepare-commit-msg

set -e

# Keep the full-path filenames consistent to not confuse code coverage tools.
docker run -v `pwd`:`pwd` -w `pwd` scrapertest \
  ./git-hooks/prepare-commit-msg $1
