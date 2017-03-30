#!/usr/bin/env python
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

# No docstrings required for tests.
# Tests need to be methods of classes to aid in organization of tests. Using
#   the 'self' variable is not required.
# Too many public methods here means "many tests", which is good not bad.
# This code is in a subdirectory, but is intended to stand alone, so it uses
#   what look like relative imports to the linter
# pylint: disable=missing-docstring, no-self-use, too-many-public-methods
# pylint: disable=relative-import

import unittest

import StringIO
import mock
import testfixtures

import sync


class TestSync(unittest.TestCase):

    def setUp(self):
        class FakeEntity(dict):

            def __init__(self, key, kv_pairs):
                dict.__init__(self, **kv_pairs)
                self.key = mock.Mock()
                self.key.name = key

        self.test_datastore_data = [
            FakeEntity(
                u'rsync://utility.mlab.mlab4.prg01.'
                'measurement-lab.org:7999/switch',
                {u'lastsuccessfulcollection': 'x2017-03-28',
                 u'errorsincelastsuccessful': '',
                 u'lastcollectionattempt': 'x2017-03-29-21:22',
                 u'maxrawfilemtimearchived': 1490746201L}),
            FakeEntity(
                u'rsync://utility.mlab.mlab4.prg01.'
                'measurement-lab.org:7999/utilization',
                {u'errorsincelastsuccessful': '',
                 u'lastsuccessfulcollection': 'x2017-03-28',
                 u'lastcollectionattempt': 'x2017-03-29-21:04',
                 u'maxrawfilemtimearchived': 1490746202L}),
            FakeEntity(
                u'rsync://utility.mlab.mlab4.sea02.'
                'measurement-lab.org:7999/switch',
                {u'lastcollectionattempt': 'x2017-03-29-15:46',
                 u'errorsincelastsuccessful':
                     '[2017-03-29 15:49:07,364 ERROR run_scraper.py:196] '
                     'Scrape and upload failed: 1'})]

    def test_parse_args_no_spreadsheet(self):
        with self.assertRaises(SystemExit):
            with testfixtures.OutputCapture() as _:
                sync.parse_args([])

    def test_parse_args_help(self):
        with self.assertRaises(SystemExit):
            with testfixtures.OutputCapture() as _:
                sync.parse_args(['-h'])

    def test_parse_args(self):
        args = sync.parse_args(['--spreadsheet', 'hello'])
        self.assertEqual(args.spreadsheet, 'hello')
        self.assertTrue(args.expected_upload_interval > 0)
        self.assertIs(type(args.datastore_namespace), str)
        self.assertIs(type(args.prometheus_port), int)
        self.assertIs(type(args.webserver_port), int)

    @mock.patch.object(sync, 'datastore')
    def test_get_fleet_data(self, mock_datastore):
        mock_client = mock.Mock()
        mock_datastore.Client.return_value = mock_client
        mock_client.query().fetch.return_value = self.test_datastore_data
        returned_answers = sync.get_fleet_data('scraper')
        correct_answers = [
            {u'dropboxrsyncaddress': u'rsync://utility.mlab.mlab4.prg01.'
                                     'measurement-lab.org:7999/switch',
             u'contact': '',
             u'lastsuccessfulcollection': 'x2017-03-28',
             u'errorsincelastsuccessful': '',
             u'lastcollectionattempt': 'x2017-03-29-21:22',
             u'maxrawfilemtimearchived': 1490746201L},
            {u'dropboxrsyncaddress': u'rsync://utility.mlab.mlab4.prg01.'
                                     'measurement-lab.org:7999/utilization',
             u'contact': '',
             u'errorsincelastsuccessful': '',
             u'lastsuccessfulcollection': 'x2017-03-28',
             u'lastcollectionattempt': 'x2017-03-29-21:04',
             u'maxrawfilemtimearchived': 1490746202L},
            {u'dropboxrsyncaddress': u'rsync://utility.mlab.mlab4.sea02'
                                     '.measurement-lab.org:7999/switch',
             u'contact': '',
             u'errorsincelastsuccessful':
                 '[2017-03-29 15:49:07,364 ERROR run_scraper.py:196] '
                 'Scrape and upload failed: 1',
             u'lastsuccessfulcollection': '',
             u'lastcollectionattempt': 'x2017-03-29-15:46',
             u'maxrawfilemtimearchived': ''}]
        self.assertEqual(returned_answers, correct_answers)

    @mock.patch.object(sync, 'datastore')
    def test_do_get(self, mock_datastore):
        mock_client = mock.Mock()
        mock_datastore.Client.return_value = mock_client
        mock_client.query().fetch.return_value = self.test_datastore_data
        mock_handler = mock.Mock(sync.WebHandler)
        mock_handler.wfile = StringIO.StringIO()
        sync.WebHandler.do_GET(mock_handler)
        self.assertEqual(mock_handler.wfile.getvalue().count('<tr>'), 4)

    @mock.patch.object(sync, 'datastore')
    def test_do_get_no_data(self, mock_datastore):
        mock_client = mock.Mock()
        mock_datastore.Client.return_value = mock_client
        mock_client.query().fetch.return_value = []
        mock_handler = mock.Mock(sync.WebHandler)
        mock_handler.wfile = StringIO.StringIO()
        sync.WebHandler.do_GET(mock_handler)
        self.assertEqual(mock_handler.wfile.getvalue().count('<td>'), 0)

    @mock.patch.object(sync, 'datastore')
    def test_spreadsheet(self, mock_datastore):
        mock_client = mock.Mock()
        mock_datastore.Client.return_value = mock_client
        mock_client.query().fetch.return_value = self.test_datastore_data
        mock_spreadsheet_service = mock.Mock()
        sheet = sync.Spreadsheet(mock_spreadsheet_service, 'test_id')
        sheet.update(sync.get_fleet_data('test_namespace'))

    def test_docstring_exists(self):
        self.assertIsNotNone(sync.__doc__)
