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

run-scraper.py is intended to be the CMD for the docker container in which the
scraper runs.  Every argument that run-scraper does not parse is passed through
verbatim to scraper.py.  By default, run-scraper will try to scrape the given
target every half an hour (on average, but with exponential jitter to assure a
memoryless distribution of runtimes).  run-scraper reports its status both via
logging messages and a Prometheus metrics port.
"""

import argparse
import logging
import random
import sys
import subprocess
import time

import prometheus_client

# Prometheus histogram buckets are web-response-sized by default, with lots of
# sub-second buckets and very few multi-second buckets.  We need to change them
# to rsync-download-sized, with lots of multi-second buckets up to even a
# multi-hour bucket or two.  The precise choice of bucket values below is a
# compromise between exponentially-sized bucket growth and a desire to make
# sure that the bucket sizes are nice round time units.
TIME_BUCKETS = (1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
                1800.0, 3600.0, 7200.0, float('inf'))

# The monitoring variables exported by the prometheus_client
# The prometheus_client libraries confuse the linter.
# pylint: disable=no-value-for-parameter
RUNS = prometheus_client.Histogram(
    'scraper_run_time_seconds',
    'How long each run of the scraper took',
    buckets=TIME_BUCKETS)
SLEEPS = prometheus_client.Histogram(
    'scraper_sleep_time_seconds',
    'How long we slept between scraper runs (should be an exp distribution)',
    buckets=TIME_BUCKETS)
# pylint: enable=no-value-for-parameter
RETURN_CODES = prometheus_client.Counter(
    'scraper_return_code',
    'How many times have we seen each shell return code?',
    ['return_code'])


# TODO(https://github.com/m-lab/scraper/issues/11) no scraper.py subprocess
#
# Integrate run_scraper.py and scraper.py to make the scraper a function call
# instead of a subprocess. This will allow for finer-grained monitoring, allow
# scraper to be better unit-tested, and basically is a better srchitecture.  To
# do this: Integrate the argument parsing, convert scraper.main into init(),
# download(), and upload_if_needed(), make run_scraper use those methods, and
# then add prometheus metrics as needed to each subpart.

def parse_known_args(argv):  # pragma: no cover
    """Parse all the arguments we know how to parse.

    All remaining (unparsed) arguments should be passed on to the scraper.
    """
    parser = argparse.ArgumentParser(
        description='Run the scraper.py program in a loop.  All arguments '
                    'passed in that are not specified below will be passed '
                    'directly through to the scraper.py invocation')
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
    return parser.parse_known_args(argv)


def main(argv):  # pragma: no cover
    """Run scraper.py in an infinite loop."""
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s %(levelname)s %(filename)s:%(lineno)d] '
        '%(message)s')
    parsed_args, scraper_args = parse_known_args(argv[1:])
    prometheus_client.start_http_server(parsed_args.metrics_port)
    command_line = ['/scraper.py'] + scraper_args
    logging.info('Calling "%s" in a loop', ' '.join(command_line))
    # We sleep before we scrape to ameliorate the thundering herd problem when
    # the jobs are all first run.
    while True:
        # In order to prevent a thundering herd of rsync jobs, we should spread
        # the jobs around in a memoryless way.  By choosing our inter-job sleep
        # time from an exponential distribution, we ensure that the resulting
        # time distribution of jobs is Poisson, the one and only memoryless
        # distribution.  The denominator of the fraction in the code below is
        # the mean sleep time in seconds.
        sleep_time = random.expovariate(1.0 / parsed_args.expected_wait_time)
        logging.info('Sleeping for %g seconds', sleep_time)
        with SLEEPS.time():
            time.sleep(sleep_time)
        try:
            logging.info('Scraping')
            with RUNS.time():
                subprocess.check_call(command_line)
            logging.info('Scraped (and possibly uploaded) successfully')
            # pylint: disable=no-member
            RETURN_CODES.labels(return_code='0').inc()
            # pylint: enable=no-member
        except subprocess.CalledProcessError as error:
            logging.error('Scraper failed! command_line=%s, exit code=%d',
                          ' '.join(command_line), error.returncode)
            # pylint: disable=no-member
            RETURN_CODES.labels(return_code=str(error.returncode)).inc()
            # pylint: enable=no-member


if __name__ == '__main__':  # pragma: no cover
    main(sys.argv)
