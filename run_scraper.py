#!/usr/bin/python
# Copyright 2017 Scraper Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# TODO(https://github.com/m-lab/scraper/issues/9) end-to-end tests

"""This program runs the MLab scraper in a loop.

run_scraper.py is intended to be the CMD for the docker container in which the
scraper runs.  By default, run-scraper will try to scrape the given target every
half an hour (on average, but with exponential jitter to assure a memoryless
distribution of runtimes).  run-scraper reports its status both via logging
messages and a Prometheus metrics port.
"""

import argparse
import logging
import random
import sys
import time

import googleapiclient
import oauth2client
import prometheus_client
import scraper

# The monitoring variables exported by the prometheus_client
# The prometheus_client libraries confuse the linter.
# pylint: disable=no-value-for-parameter
RSYNC_RUNS = prometheus_client.Histogram(
    'scraper_rsync_runtime_seconds',
    'How long each rsync download took',
    buckets=scraper.TIME_BUCKETS)
UPLOAD_RUNS = prometheus_client.Histogram(
    'scraper_gcs_upload_runtime_seconds',
    'How long each GCS upload took',
    buckets=scraper.TIME_BUCKETS)
SLEEPS = prometheus_client.Histogram(
    'scraper_sleep_time_seconds',
    'How long we slept between scraper runs (should be an exp distribution)',
    buckets=scraper.TIME_BUCKETS)
# pylint: enable=no-value-for-parameter
SCRAPER_SUCCESS = prometheus_client.Counter(
    'scraper_success',
    'How many times has the scraper died, how many times has it succeeded?',
    ['message'])


def parse_cmdline(args):
    """Parse the commandline arguments.

    Args:
      args: the command-line arguments, minus the name of the binary

    Returns:
      the results of ArgumentParser.parse_args
    """
    parser = argparse.ArgumentParser(
        parents=[oauth2client.tools.argparser],
        description='Repeatedly scrape a single experiment at a site, uploading'
                    'the results once enough time has passed.')
    parser.add_argument(
        '--expected_wait_time',
        metavar='SECONDS',
        type=float,
        default=1800,
        help='The average number of seconds to wait between runs of '
        'scraper.py. The exact time waited will vary in order to generate '
        'a memoryless distribution of scraper.py runtimes, but this specifies '
        'the mean of that distribution.  By default it is 1800 seconds (30 '
        'minutes).')
    parser.add_argument(
        '--metrics_port',
        metavar='PORT',
        type=int,
        default=9090,
        help='The port on which Prometheus metrics are exported.')
    parser.add_argument(
        '--rsync_host',
        metavar='HOST',
        type=scraper.assert_mlab_hostname,
        required=True,
        help='The host to connect to over rsync')
    parser.add_argument(
        '--rsync_module',
        metavar='MODULE',
        type=str,
        required=True,
        help='The rsync module to connect to on the server')
    parser.add_argument(
        '--data_dir',
        metavar='DIR',
        type=str,
        required=True,
        help='The directory under which to save the data')
    parser.add_argument(
        '--rsync_binary',
        metavar='RSYNC',
        type=str,
        default='/usr/bin/rsync',
        required=False,
        help='The location of the rsync binary (default is /usr/bin/rsync)')
    parser.add_argument(
        '--rsync_port',
        metavar='PORT',
        type=int,
        default=7999,
        required=False,
        help='The port on which the rsync server runs (default is 7999)')
    parser.add_argument(
        '--datastore_namespace',
        metavar='NAMESPACE',
        type=str,
        default='scraper',
        help='The cloud datastore namespace to use in the current project. '
             'Every google cloud project has one datastore associated with '
             'it. In order for us to run multiple scrapers within the same '
             'cloud project, we add a "namespace" element to every key. This '
             'way, independent parallel deployments can use the same datastore '
             'and not need independent projects.  To run a separate '
             'independent scraper in the same project, choose a different '
             'argument for the datastore_namespace. Otherwise, the same '
             'datastore entries will be being updated by two independent '
             'scrapers, and then the nodes might delete data before the '
             'authoritative scraper has successfully scraped it off of them.')
    parser.add_argument(
        '--tar_binary',
        metavar='TAR',
        type=str,
        default='/bin/tar',
        required=False,
        help='The location of the tar binary (default is /bin/tar)')
    parser.add_argument(
        '--max_uncompressed_size',
        metavar='SIZE',
        type=int,
        default=1000000000,
        required=False,
        help='The maximum number of bytes in an uncompressed tarfile (default '
        'is 1,000,000,000 = 1 GB)')
    parser.add_argument(
        '--bucket',
        metavar='BUCKET',
        type=str,
        default='mlab-storage-scraper-test',
        help='The Google Cloud Storage bucket to upload to')
    return parser.parse_args(args)


def main(argv):  # pragma: no cover
    """Run scraper.py in an infinite loop."""
    args = parse_cmdline(argv[1:])
    rsync_url, status, destination, storage_service = scraper.init(args)
    prometheus_client.start_http_server(args.metrics_port)
    while True:
        try:
            logging.info('Scraping %s', rsync_url)
            with RSYNC_RUNS.time():
                scraper.download(args, rsync_url, status, destination)
            with UPLOAD_RUNS.time():
                scraper.upload_if_allowed(args, status, destination,
                                          storage_service)
            SCRAPER_SUCCESS.labels(message='success').inc()
        except (SystemExit, AssertionError,
                googleapiclient.errors.HttpError) as error:
            logging.error('Scrape and upload failed: %s', error.message)
            SCRAPER_SUCCESS.labels(message=str(error.message)).inc()
        # In order to prevent a thundering herd of rsync jobs, we spread the
        # jobs around in a memoryless way.  By choosing our inter-job sleep
        # time from an exponential distribution, we ensure that the resulting
        # time distribution of jobs is Poisson, the one and only memoryless
        # distribution.  The denominator of the fraction in the code below is
        # the mean sleep time in seconds.
        #
        # That said, don't sleep for more than two hours.
        sleep_time = min(random.expovariate(1.0 / args.expected_wait_time),
                         7200)
        logging.info('Sleeping for %g seconds', sleep_time)
        with SLEEPS.time():
            time.sleep(sleep_time)


if __name__ == '__main__':  # pragma: no cover
    main(sys.argv)
