#!/bin/bash
#
# We have to do our testing inside a container, so first we build the container,
# then we build the container that has the unit tests added.  If the build
# passes, then so does git-hooks/python-pre-commit
#   ln -s ../../pre-commit.sh .git/hooks/pre-commit

set -e

docker build . -f Dockerfile -t scraper
docker build . -f TestDockerfile -t scrapertest
# Keep the full-path filenames consistent inside and outside the container in an
# effort to not confuse code coverage tools.
docker run -v `pwd`:`pwd` -w `pwd` scrapertest ./run_tests_with_emulator.sh
