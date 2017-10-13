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

# No docstrings required for tests, and tests need to be methods of classes to
# aid in organization of tests. Using the 'self' variable is not required.
#
# pylint: disable=missing-docstring, no-self-use, too-many-public-methods

import datetime
import os
import shutil
import unittest

import apiclient
import mock
import requests
import testfixtures

from oauth2client.contrib import gce

# pylint: disable=no-name-in-module,relative-import
import google.auth.credentials
from google.cloud import datastore
# pylint: enable=no-name-in-module,relative-import

import run_scraper
import scraper


class TestRunScraper(unittest.TestCase):

    def test_args(self):
        rsync_host = 'mlab1.dne0t.measurement-lab.org'
        rsync_module = 'ndt'
        data_dir = '/tmp/bigplaceforbackup'
        rsync_binary = '/usr/bin/rsync'
        rsync_port = 1234
        max_uncompressed_size = 1024
        args = run_scraper.parse_cmdline([
            '--rsync_host', rsync_host, '--rsync_module', rsync_module,
            '--data_dir', data_dir, '--rsync_binary', rsync_binary,
            '--rsync_port', str(rsync_port), '--max_uncompressed_size',
            str(max_uncompressed_size)
        ])
        self.assertEqual(args.rsync_host, rsync_host)
        self.assertEqual(args.rsync_module, rsync_module)
        self.assertEqual(args.data_dir, data_dir)
        self.assertEqual(args.rsync_binary, rsync_binary)
        self.assertEqual(args.rsync_port, rsync_port)
        self.assertEqual(args.max_uncompressed_size, max_uncompressed_size)
        args = run_scraper.parse_cmdline([
            '--rsync_host', rsync_host, '--rsync_module', rsync_module,
            '--data_dir', data_dir
        ])
        self.assertEqual(args.rsync_binary, '/usr/bin/rsync')
        self.assertEqual(args.rsync_port, 7999)
        self.assertEqual(args.max_uncompressed_size, 1000000000)
        self.assertEqual(args.num_runs, float('inf'))

    def test_args_help(self):
        with self.assertRaises(SystemExit):
            with testfixtures.OutputCapture() as _:
                run_scraper.parse_cmdline(['-h'])


class EmulatorCreds(google.auth.credentials.Credentials):
    """A mock credential object.

    Used to avoid the need for auth entirely when using local versions of cloud
    services.

    Based on:
       https://github.com/GoogleCloudPlatform/google-cloud-python/blob/3caed41b88eb58673ee5c3396afa3f8fff97d4d4/test_utils/test_utils/system.py#L33
    """

    def refresh(self, _request):  # pragma: no cover
        raise RuntimeError('Should never be called.')


# Nota bene: We can't use freezegun in the class because it hopelessly confuses
# the emulated servers.
class EndToEndWithFakes(unittest.TestCase):
    prometheus_port = 9089

    def setUp(self):
        EndToEndWithFakes.prometheus_port += 1

        # The directory of files on the server rsync directory.
        self.rsync_data_dir = '/tmp/iupui_ndt/'
        # rmtree removes the directory, so we need to use shell here.
        self.addCleanup(lambda: os.system('rm -Rf /tmp/iupui_ndt/*'))

        # The directory that serves as a fake Google Cloud Storage bucket.
        self.cloud_upload_dir = '/tmp/cloud_storage_bucket/'
        os.makedirs(self.cloud_upload_dir)
        self.addCleanup(lambda: shutil.rmtree('/tmp/cloud_storage_bucket'))

        # Patch credentials to do nothing. The local datastore emulator doesn't
        # even look at creds.
        creds_patcher = mock.patch.object(
            gce, 'AppAssertionCredentials',
            return_value=EmulatorCreds())
        creds_patcher.start()
        self.addCleanup(creds_patcher.stop)
        fake_datastore_client = datastore.Client(project='mlab-sandbox',
                                                 namespace='test',
                                                 credentials=EmulatorCreds(),
                                                 _http=requests.Session())
        # Make datastore clients connect to the local datastore emulator
        datastore_client_patcher = mock.patch.object(
            datastore, 'Client', return_value=fake_datastore_client)
        datastore_client_patcher.start()
        self.addCleanup(datastore_client_patcher.stop)

        # Empty out the local datastore emulator.
        query = fake_datastore_client.query(kind='dropboxrsyncaddress')
        entities = query.fetch()
        for entity in entities:
            fake_datastore_client.delete(entity.key)

        # Make an entirely mocked storage service
        self.mock_storage = mock.MagicMock()
        discovery_build_patcher = mock.patch.object(
            apiclient.discovery, 'build', return_value=self.mock_storage)
        discovery_build_patcher.start()
        self.addCleanup(discovery_build_patcher.stop)

        class FakeProgress(object):
            def __init__(self, status):
                self.status = status

            def progress(self):
                return self.status

        surrounding_testcase = self

        class FakeRequest(object):
            """Handle upload requests by copying the file."""

            def __init__(self, **kwargs):
                self.index = -1
                # pylint: disable=protected-access
                shutil.copy(kwargs['media_body']._filename,
                            surrounding_testcase.cloud_upload_dir)
                # pylint: enable=protected-access
                self.return_values = [
                    (FakeProgress(.5), None),
                    (FakeProgress(1.0), mock.MagicMock())]

            def next_chunk(self):
                self.index += 1
                return self.return_values[self.index]

        inserter = mock.MagicMock()
        inserter.insert.side_effect = FakeRequest
        self.mock_storage.objects.return_value = inserter

        # Keep filenames unique
        self.file_index = 0

    def create_file(self, filetime):
        """Make a 1k file in the right place with mtimes of the filetime."""
        self.file_index += 1
        subd = '%d/%02d/%02d/' % (filetime.year, filetime.month, filetime.day)
        fullpath = self.rsync_data_dir + subd
        if not os.path.isdir(fullpath):
            os.makedirs(fullpath)
        filename = filetime.strftime(fullpath + '%Y%m%dT%H:%M:%S.%fZ_.' +
                                     str(self.file_index))
        thefile = open(filename, 'w+')
        thefile.write('a' * 1024)
        thefile.close()
        filetime_seconds = int(
            (filetime - datetime.datetime(1970, 1, 1)).total_seconds())
        os.utime(filename, (filetime_seconds, filetime_seconds))

    @mock.patch('time.sleep')
    def test_main_breaks_up_big_tarfiles(self, _mock_sleep):
        # Add files for yesterday and today
        self.create_file(
            datetime.datetime.now() - datetime.timedelta(days=1, hours=9))
        self.create_file(
            datetime.datetime.now() - datetime.timedelta(days=1, hours=9))

        # Should get two tarfiles uploaded, because yesterday's data won't fit
        # in a single tarfile.
        run_scraper.main([
            'run_as_e2e_test',
            '--num_runs', '1',
            '--rsync_host', 'ndt.iupui.mlab4.xxx08.measurement-lab.org',
            '--rsync_module', 'iupui_ndt',
            '--data_dir', '/scraper_data',
            '--metrics_port', str(EndToEndWithFakes.prometheus_port),
            '--max_uncompressed_size', '1024'])

        # Verify that the storage service received the files
        tgzfiles = os.listdir(self.cloud_upload_dir)
        self.assertEqual(len(tgzfiles), 2)

    @mock.patch('time.sleep')
    def test_main(self, _mock_sleep):
        # Add files for yesterday and today. Only yesterday should get uploaded.
        self.create_file(
            datetime.datetime.now() - datetime.timedelta(days=1, hours=9))
        self.create_file(datetime.datetime.now())

        # Should get one tarfile uploaded, because today's data is too new.
        run_scraper.main([
            'run_as_e2e_test',
            '--num_runs', '1',
            '--rsync_host', 'ndt.iupui.mlab4.xxx08.measurement-lab.org',
            '--rsync_module', 'iupui_ndt',
            '--data_dir', '/scraper_data',
            '--metrics_port', str(EndToEndWithFakes.prometheus_port),
            '--max_uncompressed_size', '1024'])

        # Verify that the storage service received the files
        tgzfiles = os.listdir(self.cloud_upload_dir)
        self.assertEqual(len(tgzfiles), 1)

    @mock.patch.object(scraper, 'download')
    @mock.patch('time.sleep')
    def test_main_with_recoverable_failure(self, _mock_sleep, mock_download):
        mock_download.side_effect = scraper.RecoverableScraperException(
            'fake_label', 'faked_exception')

        # Verify that the recoverable exception does not rise to the top level
        run_scraper.main([
            'run_as_e2e_test',
            '--num_runs', '1',
            '--rsync_host', 'ndt.iupui.mlab4.xxx08.measurement-lab.org',
            '--rsync_module', 'iupui_ndt',
            '--data_dir', '/scraper_data',
            '--metrics_port', str(EndToEndWithFakes.prometheus_port),
            '--max_uncompressed_size', '1024'])

    @mock.patch('time.sleep')
    def test_main_with_no_data(self, mock_sleep):
        slept_seconds = []
        mock_sleep.side_effect = slept_seconds.append

        # Verify that the recoverable exception does not rise to the top level
        run_scraper.main([
            'run_as_e2e_test',
            '--num_runs', '1',
            '--rsync_host', 'ndt.iupui.mlab4.xxx08.measurement-lab.org',
            '--rsync_module', 'iupui_ndt',
            '--data_dir', '/scraper_data',
            '--metrics_port', str(EndToEndWithFakes.prometheus_port),
            '--max_uncompressed_size', '1024'])

        # Verify that the sleep time is never too long
        for time_slept in slept_seconds:
            self.assertLessEqual(time_slept, 3600)


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
