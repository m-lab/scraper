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

"""This program uploads the status in cloud datastore to a spreadsheet.

Nodes in the MLab fleet use a spreadsheet to determine what data is and is not
safe to delete. Unfortunately, if every scraper just wrote to that spreadsheet,
then we would quickly run out of spreadsheet API quota.  Also, the spreadsheet
is kind of a janky hack for what really should be a key-value store. The new
scraper has its source of truth in a key-value store (Google Cloud Datastore),
and this program has the job of updating the spreadsheet with that truth.  In a
longer term migration, this script and the spreadsheet should both be
eliminated, and the scripts in charge of data deletion should read from cloud
datastore directly.

This program needs to be run on a GCE instance that has access to the Sheets
API  Sheets API access is not enabled by default for GCE, and it can't be
enabled from the web-based GCE instance creation interface.  Worse, the scopes
that a GCE instance has can't be changed after creation. To create a new GCE
instance named scraper-dev that has access to both cloud APIs and spreadsheet
apis, you could use the following command line:
   gcloud compute instances create scraper-dev \
       --scopes cloud-platform,https://www.googleapis.com/auth/spreadsheets
"""

import argparse
import sys
import logging

# pylint: disable=duplicate-code
def parse_args(argv):
    """Parses the command-line arguments.

    Args:
        argv: the list of arguments, minus the name of the binary

    Returns:
        A dictionary-like object containing the results of the parse.
    """
    parser = argparse.ArgumentParser(
        description='Repeatedly upload the synchronization data in Cloud '
                    'Datastore up to the specified spreadsheet.')
    parser.add_argument(
        '--expected_upload_interval',
        metavar='SECONDS',
        type=float,
        default=300,
        help='The number of seconds to wait between uploads (on average).  We '
             'add jitter to this number to prevent the buildup of patterns, '
             'but this specifies the average of the wait times.')
    parser.add_argument(
        '--spreadsheet',
        metavar='SPREADSHEET_ID',
        type=str,
        required=True,
        help='The ID of the spreadsheet to update')
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
             'and not need independent projects.')
    parser.add_argument(
        '--port',
        metavar='PORT',
        type=int,
        default=8000,
        help='The port on which both metrics and a summary of the sheet are '
             'exported.')
    return parser.parse_args(argv)
# pylint: enable=duplicate-code


def main(argv):
    """Update the spreadsheet in a loop.

    Set up the logging, parse the command line, set up monitoring, set up the
    datastore client, set up the spreadsheet client, set up the webserver, and
    then repeatedly update the spreadsheet and sleep.
    """
    # Set up logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s %(levelname)s %(filename)s:%(lineno)d] '
               '%(message)s')
    # Parse the commandline
    args = parse_args(argv[1:])
    print args, "DELETE THIS LINE"
    # Set up monitoring
    # Set up datastore client
    # Set up spreadsheet client
    # Set up the webserver
    # Repeatedly copy information from the datastore to the spreadsheet

if __name__ == '__main__':
    main(sys.argv)
