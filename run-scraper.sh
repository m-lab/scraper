#!/bin/bash

RSYNC_HOST=$1

while true; do
  /scraper.py --rsync_host=${RSYNC_HOST} --lockfile_dir=locks \
      --rsync_module=ndt --data_dir=tmp \
      --spreadsheet=143pU25GJidW2KZ_93hgzHdqTqq22wgdxR_3tt3dvrJY \
      --rsync_binary=/usr/bin/rsync 
  # Lose any and all data races with stdout and stderr from the preceding
  # process in order to ensure that all subsequent messages happen after.
  sleep 1
  # In order to prevent a thundering herd of rsync jobs, we should spread the
  # jobs around in a memoryless way.  By choosing our inter-job sleep time from
  # an exponential distribution, we ensure that the resulting time distribution
  # of jobs is poisson, the memoryless distribution.  The denominator of the
  # fraction in the code below is the mean sleep time in seconds.
  SLEEP_INTERVAL=$(python -c 'import random; print int(random.expovariate(1.0/1800))')
  echo "Sleeping for ${SLEEP_INTERVAL} seconds between jobs to ${RSYNC_HOST}"
  sleep ${SLEEP_INTERVAL}
done
