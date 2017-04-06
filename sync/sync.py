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
eliminated, and the scripts in charge of data deletion should read from a
low-latency source of cloud datastore data.

This program needs to be run on a GCE instance that has access to the Sheets
API.  Sheets API access is not enabled by default for GCE, and it can't be
enabled from the web-based GCE instance creation interface.  Worse, the scopes
that a GCE instance has can't be changed after creation. To create a new GCE
instance named scraper-dev that has access to both cloud APIs and spreadsheet
apis, you could use the following command line:
   gcloud compute instances create scraper-dev \
       --scopes cloud-platform,https://www.googleapis.com/auth/spreadsheets
"""

import argparse
import BaseHTTPServer
import logging
import random
import SocketServer
import sys
import textwrap
import thread
import time
import traceback

import apiclient
from google.cloud import datastore
from oauth2client.contrib import gce

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
    'spreadsheet_sync_runtime_seconds',
    'How long each sheet sync took',
    buckets=TIME_BUCKETS)
SLEEPS = prometheus_client.Histogram(
    'spreadsheet_sync_sleep_time_seconds',
    'Sleep time between sheet update runs (should be an exp distribution)',
    buckets=TIME_BUCKETS)
# pylint: enable=no-value-for-parameter
SUCCESS = prometheus_client.Counter(
    'spreadsheet_sync_success',
    'How many times has the sheet update succeeded and failed',
    ['message'])


class SyncException(Exception):
    """The exceptions this system raises."""


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
        help='The cloud datastore namespace to use in the current project.')
    parser.add_argument(
        '--prometheus_port',
        metavar='PORT',
        type=int,
        default=9090,
        help='The port on which metrics are exported.')
    parser.add_argument(
        '--webserver_port',
        metavar='PORT',
        type=int,
        default=80,
        help='The port on which a summary of the sheet is exported.')
    return parser.parse_args(argv)


KEYS = ['dropboxrsyncaddress', 'contact', 'lastsuccessfulcollection',
        'errorsincelastsuccessful', 'lastcollectionattempt',
        'maxrawfilemtimearchived']


def get_fleet_data(namespace):
    """Returns a list of dictionaries, one for every entry in the namespace."""
    datastore_client = datastore.Client(namespace=namespace)
    answers = []
    for item in datastore_client.query(kind='rsync_url').fetch():
        answer = {}
        answer[KEYS[0]] = item.key.name
        for k in KEYS[1:]:
            answer[k] = item.get(k, '')
        answers.append(answer)
    return answers


class WebHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """Print the ground truth from cloud datastore."""
    namespace = 'test'

    # The name is inherited, so we have to use it even if pylint hates it.
    # pylint: disable=invalid-name
    def do_GET(self):
        """Print out the ground truth from cloud datastore as a webpage."""
        logging.info('Request from %s', self.client_address)
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        print >> self.wfile, textwrap.dedent('''\
        <html>
        <head>
          <title>MLab Scraper Status</title>
          <style>
            table {
              border-collapse: collapse;
              margin-left: auto;
              margin-right: auto;
            }
            tr:nth-child(even) {
              background-color: #FFF;
            }
            tr:nth-child(even) {
              background-color: #EEE;
            }
          </style>
        </head>
        <body>
          <table><tr>''')
        try:
            data = get_fleet_data(WebHandler.namespace)
        # This will be used for debugging errors, so catching an overly-broad
        # exception is apprpriate.
        # pylint: disable=broad-except
        except Exception as e:
            logging.error('Unable to retrieve data from datastore: %s', str(e))
            print >> self.wfile, '</table>'
            print >> self.wfile, '<p>Datastore error:</p><pre>'
            traceback.print_exc(file=self.wfile)
            print >> self.wfile, '</pre></body></html>'
            return
        # pylint: enable=broad-except

        if not data:
            print >> self.wfile, '</table><p>NO DATA</p>'
            print >> self.wfile, '</body></html>'
            return
        else:
            for key in KEYS:
                print >> self.wfile, '     <th>%s</th>' % key
            print >> self.wfile, '  </tr>'
            rows = sorted([d.get(key, '') for key in KEYS] for d in data)
            for data in rows:
                print >> self.wfile, '  <tr>'
                for item in data:
                    print >> self.wfile, '     <td>%s</td>' % item
                print >> self.wfile, '    </tr>'
            print >> self.wfile, '    </table>'
        print >> self.wfile, '  <center><small>', time.ctime()
        print >> self.wfile, '    </small></center>'
        print >> self.wfile, '</body></html>'
    # pylint: enable=invalid-name


def start_webserver_in_new_thread(port):  # pragma: no cover
    """Starts the wbeserver to serve the ground truth page.

    Code cribbed from prometheus_client.
    """
    server_address = ('', port)

    class ThreadingSimpleServer(SocketServer.ThreadingMixIn,
                                BaseHTTPServer.HTTPServer):
        """Use the threading mix-in to avoid forking or blocking."""

    httpd = ThreadingSimpleServer(server_address, WebHandler)
    thread.start_new_thread(httpd.serve_forever, ())


class Spreadsheet(object):
    """Updates a given spreadsheet to mirror cloud datastore."""

    def __init__(self, service, sheet_id,
                 worksheet='Drop box status (auto updated)'):
        self._service = service
        self._sheet_id = sheet_id
        self._worksheet = worksheet

    def _retrieve_sheet_data(self):
        """Get the current data from the sheet.

        Used to ensure that the update process does not re-order rows.

        Raises:
           SyncException: The update failed.
        """
        # Sheet ranges have the form: <worksheet name>!<col1>:<col2>
        sheet_range = self._worksheet + '!A:' + chr(ord('A') + len(KEYS))
        result = self._service.spreadsheets().values().get(
            spreadsheetId=self._sheet_id, range=sheet_range).execute()
        if 'values' not in result:
            logging.error('Spreadsheet retrieve failed (%s)', result)
            raise SyncException(
                'Could not retrieve sheet data (%s)' % str(result))
        rows = result.get('values')
        if rows:
            return rows[0], rows[1:]
        else:
            logging.warning('No data found on spreadsheet.')
            return KEYS, []

    def _update_spreadsheet(self, header, rows):
        """Sets the contents of the spreadsheet.

        Used to set the contents of the spreadsheet, overriding all existing
        items in the sheet.

        Raises:
           SyncException: The update failed.
        """
        # Sheet ranges have the form: <worksheet name>!<col1>:<col2>
        # Row numbers are 1-indexed instead of zero-indexed and the first row
        # is reserved for the header.
        update_range = (self._worksheet +
                        '!A1:' + chr(ord('A') + len(header))
                        + str(len(rows) + 1))
        body = {'values': [header] + rows}
        logging.info('About to update %s', update_range)
        response = self._service.spreadsheets().values().update(
            spreadsheetId=self._sheet_id, range=update_range,
            body=body, valueInputOption='RAW').execute()
        if not response['updatedRows']:
            logging.error('Spreadsheet update failed (%s)', response)
            raise SyncException(
                'Spreadsheet update failed (%s)' % str(response))

    def update(self, data):
        """Updates the contents of the spreadsheet.

        This respects the existing order of the rsync modules, but appends new
        rows for any rsync modules that did not previously exist in the sheet.

        Args:
          data: a list of dictionaries

        Raises:
           SyncException: The update failed.
        """
        updated_data = {d[KEYS[0]]: d for d in data}
        new_rows = []
        header, old_rows = self._retrieve_sheet_data()
        rsync_index = header.index(KEYS[0])
        for old_row in old_rows:
            rsync_url = old_row[rsync_index]
            if rsync_url in updated_data:
                new_row = [updated_data[rsync_url][h] for h in header]
                new_rows.append(new_row)
                del updated_data[rsync_url]
            else:
                new_rows.append(old_row)
        for rsync_url in sorted(updated_data):
            new_row = [updated_data[rsync_url][h] for h in header]
            new_rows.append(new_row)
        return self._update_spreadsheet(header, new_rows)


def main(argv):  # pragma: no cover
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
    WebHandler.namespace = args.datastore_namespace
    # Set up spreadsheet client
    creds = gce.AppAssertionCredentials()
    discovery_url = ('https://sheets.googleapis.com/$discovery/rest?'
                     'version=v4')
    sheets_service = apiclient.discovery.build(
        'sheets', 'v4', discoveryServiceUrl=discovery_url,
        credentials=creds, cache_discovery=False)
    spreadsheet = Spreadsheet(sheets_service, args.spreadsheet)
    # Set up the monitoring
    prometheus_client.start_http_server(args.prometheus_port)
    start_webserver_in_new_thread(args.webserver_port)
    # Repeatedly copy information from the datastore to the spreadsheet
    while True:
        # This code may be subject to transient errors in cloud datastore or
        # the sheets service. Intermittent failures of those services should
        # not crash this client, so we catch all Exceptions and log them
        # instead of crashing.
        # pylint: disable=broad-except
        try:
            with RUNS.time():
                # Download
                data = get_fleet_data(args.datastore_namespace)
                # Upload
                spreadsheet.update(data)
            SUCCESS.labels(message='success').inc()
        except (SyncException, Exception) as error:
            SUCCESS.labels(message=str(error.message)).inc()
        # pylint: enable=broad-except
        # Sleep
        sleep_time = random.expovariate(1.0 / args.expected_upload_interval)
        logging.info('Sleeping for %g seconds', sleep_time)
        with SLEEPS.time():
            time.sleep(sleep_time)


if __name__ == '__main__':  # pragma: no cover
    main(sys.argv)
