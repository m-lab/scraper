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
import logging
import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest

import freezegun
import mock
import testfixtures

import google.cloud.exceptions as cloud_exceptions

import run_scraper
import scraper


class TestScraper(unittest.TestCase):

    def setUp(self):
        # If you depend on log messages, use testfixtures.log_capture() to test
        # for their presence and assert their contents.  For the purposes of
        # the test runner, log messages on stdout/stderr are spam.
        logging.getLogger().setLevel(logging.WARNING)

    def test_one_bit(self):
        for i in range(100):
            if i in (0, 1, 2, 4, 8, 16, 32, 64):
                self.assertTrue(scraper.has_one_bit_set_or_is_zero(i), i)
            else:
                self.assertFalse(scraper.has_one_bit_set_or_is_zero(i), i)

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

    def test_args_help(self):
        with self.assertRaises(SystemExit):
            with testfixtures.OutputCapture() as _:
                run_scraper.parse_cmdline(['-h'])

    @testfixtures.log_capture()
    def test_list_rsync_files(self):
        # pylint: disable=line-too-long
        serverfiles = textwrap.dedent("""\
            opening tcp connection to ndt.iupui.mlab2.lba01.measurement-lab.org port 7999
            sending daemon args: --server --sender -vvnlogDtprze.iLsfx --timeout=300 --bwlimit=10000 . ndt/  (7 args)
            receiving incremental file list
            delta-transmission enabled
            [receiver] expand file_list pointer array to 524288 bytes, did move
            [generator] expand file_list pointer array to 524288 bytes, did move
            [receiver] expand file_list pointer array to 2097152 bytes, did move
            [generator] expand file_list pointer array to 2097152 bytes, did move
            ./
            2017/05/
            2017/06/
            2017/06/01/
            2017/06/02/
            2017/06/03/
            2017/06/03/.gz
            2017/06/03/20170603T23:59:47.143624000Z_86.164.175.237.s2c_ndttrace.gz
            2017/06/03/20170603T23:59:47.143624000Z_host86-164-175-237.range86-164.btcentralplus.com:50280.cputime.gz
            2017/06/03/20170603T23:59:47.143624000Z_host86-164-175-237.range86-164.btcentralplus.com:50280.meta
            2017/06/03/20170603T23:59:47.143624000Z_host86-164-175-237.range86-164.btcentralplus.com:50281.s2c_snaplog.gz
            2017/06/03/20170603T23:59:52.739997000Z_143.159.127.54.s2c_ndttrace.gz
            2017/06/03/20170603T23:59:53.688992000Z_195.147.32.233.c2s_ndttrace is uptodate
            [receiver] expand file_list pointer array to 1048576 bytes, did move
            [generator] expand file_list pointer array to 1048576 bytes, did move
            2017/06/03/20170603T23:59:53.688992000Z_195.147.32.233:49151.cputime
            2017/06/03/20170603T23:59:53.688992000Z_195.147.32.233:54633.c2s_snaplog
            2017/06/03/20170603T23:59:54.535055000Z_95.151.122.146.s2c_ndttrace.gz
            BADLINE
            2017/06/03/20170603T23:59:54.535055000Z_95.151.122.146:50901.cputime.gz
            2017/06/03/20170603T23:59:54.535055000Z_95.151.122.146:50901.meta
            2017/06/03/20170603T23:59:54.535055000Z_95.151.122.146:50902.s2c_snaplog.gz""")
        with tempfile.NamedTemporaryFile() as temp:
            temp.write(serverfiles)
            temp.flush()
            fake_process = subprocess.Popen(['/bin/cat', temp.name],
                                            stdout=subprocess.PIPE)
            with mock.patch.object(subprocess, 'Popen') as mock_subprocess:
                mock_subprocess.return_value = fake_process
                files = scraper.list_rsync_files('/usr/bin/rsync', 'localhost',
                                                 '/tmp')
        self.assertEqual(
            ['2017/06/03/.gz',
             '2017/06/03/20170603T23:59:47.143624000Z_86.164.175.237.s2c_ndttrace.gz',
             '2017/06/03/20170603T23:59:47.143624000Z_host86-164-175-237.range86-164.btcentralplus.com:50280.cputime.gz',
             '2017/06/03/20170603T23:59:47.143624000Z_host86-164-175-237.range86-164.btcentralplus.com:50280.meta',
             '2017/06/03/20170603T23:59:47.143624000Z_host86-164-175-237.range86-164.btcentralplus.com:50281.s2c_snaplog.gz',
             '2017/06/03/20170603T23:59:52.739997000Z_143.159.127.54.s2c_ndttrace.gz',
             '2017/06/03/20170603T23:59:53.688992000Z_195.147.32.233:49151.cputime',
             '2017/06/03/20170603T23:59:53.688992000Z_195.147.32.233:54633.c2s_snaplog',
             '2017/06/03/20170603T23:59:54.535055000Z_95.151.122.146.s2c_ndttrace.gz',
             '2017/06/03/20170603T23:59:54.535055000Z_95.151.122.146:50901.cputime.gz',
             '2017/06/03/20170603T23:59:54.535055000Z_95.151.122.146:50901.meta',
             '2017/06/03/20170603T23:59:54.535055000Z_95.151.122.146:50902.s2c_snaplog.gz'],
            files)
        # pylint: enable=line-too-long

    @mock.patch.object(subprocess, 'Popen')
    @testfixtures.log_capture()
    def test_list_rsync_files_returns_24(self, patched_subprocess):
        serverfiles = textwrap.dedent("""\
            .
            2016/
            2016/01/
            2016/01/06/
            2016/01/06/.gz
            2016/01/06/20160106T05:43:32.741066000Z_:0.cputime.gz
            2016/01/06/20160106T05:43:32.741066000Z_:0.meta
            2016/01/06/20160106T18:07:33.122784000Z_:0.cputime.gz
            BADBADBAD
            2016/01/06/20160106T18:07:33.122784000Z_:0.meta
            2016/01/06/20160106T22:31:57.229531000Z_:0.cputime.gz""")
        mock_process = mock.Mock()
        mock_process.returncode = 24
        patched_subprocess.return_value = mock_process
        mock_process.stdout = serverfiles.splitlines()
        files = scraper.list_rsync_files('/usr/bin/rsync', 'localhost', '')
        self.assertEqual([
            '2016/01/06/.gz',
            '2016/01/06/20160106T05:43:32.741066000Z_:0.cputime.gz',
            '2016/01/06/20160106T05:43:32.741066000Z_:0.meta',
            '2016/01/06/20160106T18:07:33.122784000Z_:0.cputime.gz',
            '2016/01/06/20160106T18:07:33.122784000Z_:0.meta',
            '2016/01/06/20160106T22:31:57.229531000Z_:0.cputime.gz'
        ], files)

    @mock.patch.object(subprocess, 'Popen')
    @testfixtures.log_capture()
    def test_list_rsync_files_throws_on_failure(self, patched_subprocess, log):
        mock_process = mock.Mock()
        mock_process.returncode = 1
        mock_process.stdout = []
        patched_subprocess.return_value = mock_process
        with self.assertRaises(scraper.RecoverableScraperException):
            _ = scraper.list_rsync_files('/usr/bin/rsync', 'localhost', '')
        self.assertIn('ERROR', [x.levelname for x in log.records])

    @testfixtures.log_capture()
    def test_list_rsync_files_fails(self, log):
        with self.assertRaises(scraper.RecoverableScraperException):
            scraper.list_rsync_files('/bin/false', 'localhost', '')
        self.assertIn('ERROR', [x.levelname for x in log.records])

    @testfixtures.log_capture()
    def test_remove_older_files(self, log):
        # pylint: disable=line-too-long
        files = [
            'monkey/06/.gz',
            '2016/01/06/.gz',
            '2016/01/06/20160106T05:43:32.741066000Z_:0.cputime.gz',
            '2016/01/06/20160106T05:43:32.741066000Z_:0.meta',
            '2016/01/06/20160106T18:07:33.122784000Z_:0.cputime.gz',
            '2016/01/06/20160106T18:07:33.122784000Z_:0.meta',
            '2016/01/06/20160106T22:31:57.229531000Z_:0.cputime.gz',
            '2016/10/25/20161025T17:52:59.797186000Z_eb.measurementlab.net:35192.s2c_snaplog.gz',
            '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:35192.s2c_snaplog.gz',
            '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:39482.c2s_snaplog.gz',
            '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:55050.cputime.gz',
            '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:55050.meta',
            '2016/10/26/20161026T18:02:59.898385000Z_45.56.98.222.c2s_ndttrace.gz',
            'BADYEAR/10/26/20161026T18:02:59.898385000Z_45.56.98.222.c2s_ndttrace.gz',
            '2016/10/26/20161026T18:02:59.898385000Z_45.56.98.222.s2c_ndttrace.gz',
            '2016/10/26/20161026T18:02:59.898385000Z_eb.measurementlab.net:45864.cputime.gz',
            '2016/10/26/20161026T18:02:59.898385000Z_eb.measurementlab.net:45864.meta',
            '2016/10/26/20161026T18:02:59.898385000Z_eb.measurementlab.net:50264.s2c_snaplog.gz',
            '2016/10/35/20161026T18:02:59.898385000Z_eb.measurementlab.net:50264.s2c_snaplog.gz',
            '2016/10/26/20161026T18:02:59.898385000Z_eb.measurementlab.net:52410.c2s_snaplog.gz'
        ]
        filtered = list(
            scraper.remove_older_files(
                datetime.datetime(2016, 10, 25).date(), files))
        self.assertEqual(filtered, [
            '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:35192.s2c_snaplog.gz',
            '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:39482.c2s_snaplog.gz',
            '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:55050.cputime.gz',
            '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:55050.meta',
            '2016/10/26/20161026T18:02:59.898385000Z_45.56.98.222.c2s_ndttrace.gz',
            '2016/10/26/20161026T18:02:59.898385000Z_45.56.98.222.s2c_ndttrace.gz',
            '2016/10/26/20161026T18:02:59.898385000Z_eb.measurementlab.net:45864.cputime.gz',
            '2016/10/26/20161026T18:02:59.898385000Z_eb.measurementlab.net:45864.meta',
            '2016/10/26/20161026T18:02:59.898385000Z_eb.measurementlab.net:50264.s2c_snaplog.gz',
            '2016/10/26/20161026T18:02:59.898385000Z_eb.measurementlab.net:52410.c2s_snaplog.gz'
        ])
        self.assertIn('WARNING', [x.levelname for x in log.records])
        self.assertTrue(any(any('monkey' in arg for arg in record.args)
                            for record in log.records))
        self.assertTrue(any(any('BADYEAR' in arg for arg in record.args)
                            for record in log.records))
        self.assertTrue(any(any('10/35' in arg for arg in record.args)
                            for record in log.records))
        # pylint: enable=line-too-long

    @freezegun.freeze_time('2016-10-26 18:10:00 UTC')
    @testfixtures.log_capture()
    def test_remove_too_recent_files(self):
        # pylint: disable=line-too-long
        files = [
            '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:35192.s2c_snaplog.gz',
            '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:39482.c2s_snaplog.gz',
            '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:55050.cputime.gz',
            '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:55050.meta',
            '2016/10/26/no_time_stamp.txt',
            '2016/10/26/20161026T18:02:59.898385000Z_45.56.98.222.c2s_ndttrace.gz',
            '2016/10/26/20161026T18:02:59.898385000Z_45.56.98.222.s2c_ndttrace.gz',
            '2016/10/26/20161026T18:02:59.898385000Z_eb.measurementlab.net:45864.cputime.gz',
            '2016/10/26/20161026T18:02:59.898385000Z_eb.measurementlab.net:45864.meta',
            '2016/10/26/20161026T18:02:59.898385000Z_eb.measurementlab.net:50264.s2c_snaplog.gz',
            '2016/10/26/20161026T18:02:59.898385000Z_eb.measurementlab.net:52410.c2s_snaplog.gz'
        ]
        filtered = scraper.remove_too_recent_files(files)
        self.assertItemsEqual(
            filtered,
            ['2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:35192.s2c_snaplog.gz',
             '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:39482.c2s_snaplog.gz',
             '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:55050.cputime.gz',
             '2016/10/26/20161026T17:52:59.797186000Z_eb.measurementlab.net:55050.meta',
             '2016/10/26/no_time_stamp.txt'])
        # pylint: enable=line-too-long

    @testfixtures.log_capture()
    def test_download_files_fails_and_dies(self, log):
        with self.assertRaises(scraper.RecoverableScraperException):
            scraper.download_files('/bin/false', 'localhost/',
                                   ['2016/10/26/DNE1', '2016/10/26/DNE2'],
                                   '/tmp')
        self.assertIn('ERROR', [x.levelname for x in log.records])

    @testfixtures.log_capture()
    def test_download_files_with_empty_does_nothing(self, _log):
        # If the next line doesn't raise SystemExit then the test passes
        scraper.download_files('/bin/false', 'localhost/', [], '/tmp')

    @mock.patch.object(subprocess, 'call')
    def test_download_files(self, patched_call):
        files_to_download = ['2016/10/26/DNE1', '2016/10/26/DNE2']

        def verify_contents(args):
            # Verify that the third-to-last argument to check_call is a filename
            # that contains the right data (specifically, the filenames).  This
            # test needs to be kept in sync with the order of command-line
            # arguments passed to the rsync call.
            file_with_filenames = args[-3]
            files_downloaded = file(file_with_filenames).read().split('\0')
            self.assertEqual(files_to_download, files_downloaded)
            return 0

        patched_call.side_effect = verify_contents
        self.assertEqual(patched_call.call_count, 0)
        scraper.download_files('/bin/true', 'localhost/', files_to_download,
                               '/tmp')
        self.assertEqual(patched_call.call_count, 1)

    @mock.patch.object(subprocess, 'call')
    def test_download_files_breaks_up_long_file_list(self, patched_call):
        files_to_download = ['2016/10/26/DNE.%d' % i for i in range(100070)]
        files_downloaded = []

        def verify_contents(args):
            # Verify that the third-to-last argument to check_call is a filename
            # that contains the right data (specifically, the filenames).  This
            # test needs to be kept in sync with the order of command-line
            # arguments passed to the rsync call.
            file_with_filenames = args[-3]
            files = file(file_with_filenames).read().split('\0')
            self.assertTrue(len(files) > 0)
            self.assertTrue(len(files) <= 1000)
            files_downloaded.extend(files)
            return 0

        patched_call.side_effect = verify_contents
        scraper.download_files('/bin/true', 'localhost/', files_to_download,
                               '/tmp')
        self.assertEqual(set(files_to_download), set(files_downloaded))
        self.assertEqual(patched_call.call_count, 101)

    @freezegun.freeze_time('2016-01-28 09:45:01 UTC')
    def test_new_archived_date_after_8am(self):
        self.assertEqual(scraper.max_new_archived_date(),
                         datetime.date(2016, 1, 27))

    @freezegun.freeze_time('2016-01-28 07:43:16 UTC')
    def test_new_archived_date_before_8am(self):
        self.assertEqual(scraper.max_new_archived_date(),
                         datetime.date(2016, 1, 26))

    def test_datetime_to_epoch(self):
        self.assertEqual(
            scraper.datetime_to_epoch(datetime.datetime(1970, 1, 1, 0, 0, 24)),
            24)
        self.assertEqual(
            scraper.datetime_to_epoch(datetime.datetime(1970, 1, 1)),
            0)
        self.assertEqual(
            scraper.datetime_to_epoch(datetime.datetime(1970, 1, 2)),
            24 * 60 * 60)

    def test_chdir(self):
        try:
            temp_d = tempfile.mkdtemp()
            original = os.getcwd()
            self.assertNotEqual(original, temp_d)
            with scraper.chdir(temp_d):
                self.assertEqual(os.getcwd(), temp_d)
            self.assertEqual(os.getcwd(), original)
        finally:
            shutil.rmtree(temp_d)

    def test_chdir_with_exceptions(self):
        try:
            temp_d = tempfile.mkdtemp()
            original = os.getcwd()
            self.assertNotEqual(original, temp_d)
            try:
                with scraper.chdir(temp_d):
                    self.assertEqual(os.getcwd(), temp_d)
                    raise RuntimeError()
            except RuntimeError:
                self.assertEqual(os.getcwd(), original)
        finally:
            shutil.rmtree(temp_d)

    def test_node_and_site_failure(self):
        with self.assertRaises(AssertionError):
            scraper.node_and_site('ndt.iupui.mlab1.atl02.measurement-lab.com')

    def test_node_and_site_with_prefix(self):
        self.assertEqual(
            scraper.node_and_site('ndt.iupui.mlab1.atl02.measurement-lab.org'),
            ('mlab1', 'atl02'))

    def test_node_and_site_with_suffix(self):
        self.assertEqual(
            scraper.node_and_site('mlab1.atl02.measurement-lab.org'),
            ('mlab1', 'atl02'))

    def test_get_data_caches_key(self):
        client = mock.Mock()
        client.key.return_value = {}
        status = scraper.SyncStatus(client, None)
        status.get_data()
        self.assertEqual(client.key.call_count, 1)
        self.assertEqual(client.get.call_count, 1)
        status.get_data()
        self.assertEqual(client.key.call_count, 1)
        self.assertEqual(client.get.call_count, 2)

    @testfixtures.log_capture()
    def test_get_data_robustness(self, _log):
        client = mock.Mock()
        client.key.return_value = {}
        client.get.side_effect = [
            cloud_exceptions.ServiceUnavailable('one failure'), {}]
        status = scraper.SyncStatus(client, None)
        status.get_data()

    @testfixtures.log_capture()
    def test_get_data_fails_eventually(self, _log):
        client = mock.Mock()
        client.key.return_value = {}
        client.get.side_effect = cloud_exceptions.ServiceUnavailable(
            'permanent failure')
        status = scraper.SyncStatus(client, None)
        with self.assertRaises(cloud_exceptions.ServiceUnavailable):
            status.get_data()

    @mock.patch.object(scraper.SyncStatus, 'get_data')
    def test_get_last_archived_date_from_status_default(self, patched_get):
        patched_get.return_value = None
        status = scraper.SyncStatus(None, None)
        last_archived_date = status.get_last_archived_date()
        self.assertEqual(last_archived_date, datetime.date(2009, 1, 1))

    @mock.patch.object(scraper.SyncStatus, 'get_data')
    def test_get_last_archived_date_from_status_no_date(self, patched_get):
        patched_get.return_value = dict(irrelevant='monkey')
        status = scraper.SyncStatus(None, None)
        last_archived_date = status.get_last_archived_date()
        self.assertEqual(last_archived_date, datetime.date(2009, 1, 1))

    @mock.patch.object(scraper.SyncStatus, 'get_data')
    @testfixtures.log_capture()
    def test_get_last_archived_date_bad_date(self, patched_get, log):
        status = scraper.SyncStatus(None, None)
        with self.assertRaises(scraper.NonRecoverableScraperException):
            patched_get.return_value = dict(
                lastsuccessfulcollection='2009-13-10')
            status.get_last_archived_date()
        self.assertIn('ERROR', [x.levelname for x in log.records])

    @mock.patch.object(scraper.SyncStatus, 'get_data')
    def test_get_last_archived_date_empty_date(self, patched_get):
        status = scraper.SyncStatus(None, None)
        patched_get.return_value = dict(lastsuccessfulcollection='')
        default_date = datetime.date(1970, 1, 1)
        self.assertEqual(status.get_last_archived_date(default_date),
                         default_date)

    @mock.patch.object(scraper.SyncStatus, 'get_data')
    def test_get_last_archived_date(self, patched_get):
        status = scraper.SyncStatus(None, None)
        patched_get.return_value = dict(lastsuccessfulcollection='x2010-11-02')
        last_archived_date = status.get_last_archived_date()
        self.assertEqual(last_archived_date, datetime.date(2010, 11, 2))

    @mock.patch.object(scraper.SyncStatus, 'update_data')
    def test_update_last_archived_date(self, patched_update):
        status = scraper.SyncStatus(None, None)
        status.update_last_archived_date(datetime.date(2012, 2, 29))
        self.assertEqual(patched_update.call_count, 1)
        self.assertTrue(u'x2012-02-29' in patched_update.call_args[0])
        index = patched_update.call_args[0].index(u'x2012-02-29')
        self.assertEqual(type(patched_update.call_args[0][index]), unicode)

    def test_update_data_no_value(self):
        client = mock.Mock()
        client.get.return_value = None
        status = scraper.SyncStatus(client, None)
        status.update_data('key', 'value')
        self.assertEqual(client.put.call_count, 1)

    @testfixtures.log_capture()
    def test_update_data_robustness(self, _log):
        client = mock.Mock()
        client.get.return_value = None
        client.put.side_effect = [
            cloud_exceptions.ServiceUnavailable('one failure'), None]
        status = scraper.SyncStatus(client, None)
        status.update_data('key', 'value')

    @testfixtures.log_capture()
    def test_update_data_eventually_fails(self, _log):
        client = mock.Mock()
        client.get.return_value = None
        client.put.side_effect = cloud_exceptions.ServiceUnavailable(
            'permanent failure')
        status = scraper.SyncStatus(client, None)
        with self.assertRaises(cloud_exceptions.ServiceUnavailable):
            status.update_data('key', 'value')

    def test_assert_mlab_hostname(self):
        for good_name in ['mlab4.sea02.measurement-lab.org',
                          'ndt.iupui.mlab1.nuq0t.measurement-lab.org',
                          'ndt.iupui.mlab4.nuq05.measurement-lab.org']:
            self.assertTrue(good_name, scraper.assert_mlab_hostname(good_name))
        for bad_name in ['ndt.iupui.mlab1.nuq0t.mock-lab.org',
                         'example.com',
                         'ndt.iupui.mlab01.nuq0t.measurement-lab.org',
                         'ndt.iupui.mlab1.nuqq0t.measurement-lab.org']:
            with self.assertRaises(AssertionError):
                scraper.assert_mlab_hostname(bad_name)

    @mock.patch.object(scraper.SyncStatus, 'update_data')
    def test_update_debug_msg(self, patched_update_data):
        status = scraper.SyncStatus(None, None)
        status.update_debug_message('msg')
        patched_update_data.assert_called_once_with(
            'errorsincelastsuccessful', 'msg')
        self.assertEqual(type(patched_update_data.call_args[0][1]),
                         unicode)

    @mock.patch.object(scraper.SyncStatus, 'update_data')
    def test_update_debug_msg_too_large(self, patched_update_data):
        status = scraper.SyncStatus(None, None)
        status.update_debug_message('m' * 1600)
        self.assertEqual(patched_update_data.call_count, 1)
        self.assertEqual(type(patched_update_data.call_args[0][1]),
                         unicode)
        self.assertTrue(len(patched_update_data.call_args[0][1]) < 1500)

    @freezegun.freeze_time('2016-01-28 07:43:16 UTC')
    @mock.patch.object(scraper.SyncStatus, 'update_data')
    def test_update_last_collection(self, patched_update_data):
        status = scraper.SyncStatus(None, None)
        status.update_last_collection()
        patched_update_data.assert_called_once_with('lastcollectionattempt',
                                                    'x2016-01-28-07:43')
        self.assertEqual(type(patched_update_data.call_args[0][1]),
                         unicode)

    @mock.patch.object(scraper.SyncStatus, 'update_data')
    def test_update_mtime(self, patched_update_data):
        status = scraper.SyncStatus(None, None)
        status.update_mtime(7)
        patched_update_data.assert_called_once_with(
            'maxrawfilemtimearchived', 7)

    @mock.patch.object(scraper.SyncStatus, 'update_data')
    @testfixtures.log_capture()
    def test_log_handler(self, patched_update_data, _log):
        status = scraper.SyncStatus(None, None)
        loghandler = scraper.SyncStatusLogHandler(status)
        logger = logging.getLogger('temp_test')
        logger.setLevel(logging.ERROR)
        logger.addHandler(loghandler)
        logger.info('INFORMATIVE')
        self.assertEqual(patched_update_data.call_count, 0)
        logger.error('BADNESS')
        self.assertEqual(patched_update_data.call_count, 1)
        self.assertEqual(type(patched_update_data.call_args[0][1]),
                         unicode)

    def test_run_scraper_has_docstring(self):
        # run_scraper should only be tested by end-to-end tests. However, by
        # importing it above we can at least verify that it can be parsed by
        # the python compiler.  Then, in order to not trigger the "unused
        # import" linter message, we should verify something about run_scraper.
        self.assertIsNotNone(run_scraper.__doc__)

    def test_timestamp_from_filename(self):
        self.assertEqual(
            scraper.timestamp_from_filename(
                '2017/05/10/20170510T15:05:01.520348000Z_'
                '12.30.237.162.c2s_ndttrace'),
            datetime.datetime(2017, 5, 10, 15, 5, 1, 520348))
        self.assertEqual(
            scraper.timestamp_from_filename(
                '2017/05/10/2017BAD0510T15:05:01.520348000Z_'
                '12.30.237.162.c2s_ndttrace'),
            None)

    def test_create_tarfile_template(self):
        self.assertEqual(
            scraper.create_tarfilename_template(
                datetime.date(2015, 7, 6),
                'ndt.iupui.mlab1.acc01.measurement-lab.org',
                'ndt', '/tmp'),
            '/tmp/20150706T000000Z-mlab1-acc01-ndt-%04d.tgz')


class TestScraperInTempDir(unittest.TestCase):

    def setUp(self):
        # If you depend on log messages, use testfixtures.log_capture() to test
        # for their presence and assert their contents.  For the purposes of
        # the test runner, log messages on stdout/stderr are spam.
        logging.getLogger().setLevel(logging.WARNING)
        self._old_cwd = os.getcwd()
        self.temp_d = tempfile.mkdtemp()
        os.chdir(self.temp_d)

    def tearDown(self):
        os.chdir(self._old_cwd)
        shutil.rmtree(self.temp_d)

    def test_find_all_days_to_upload_empty_okay(self):
        date = datetime.date(2016, 7, 6)
        to_upload = list(scraper.find_all_days_to_upload(self.temp_d, date))
        self.assertEqual(to_upload, [])

    @testfixtures.log_capture()
    def test_find_all_days_to_upload(self, log):
        date = datetime.date(2016, 7, 6)
        open('9000', 'w').write('hello\n')
        os.makedirs('2015/10/31')
        open('2015/9000', 'w').write('hello\n')
        open('2015/10/9000', 'w').write('hello\n')
        os.makedirs('2015/10/9001')
        os.makedirs('2016/07/05')
        os.makedirs('2016/07/07')
        os.makedirs('2016/07/monkey')
        os.makedirs('2016/monkey/monkey')
        os.makedirs('monkey/monkey/monkey')
        os.makedirs('2016/07/06')
        to_upload = list(scraper.find_all_days_to_upload(self.temp_d, date))
        self.assertEqual(to_upload, [
            datetime.date(2015, 10, 31), datetime.date(2016, 7, 5),
            datetime.date(2016, 7, 6)
        ])
        self.assertIn('ERROR', [x.levelname for x in log.records])

    def test_create_tarfile(self):
        os.makedirs('2016/01/28')
        file('2016/01/28/test1.txt', 'w').write('hello')
        file('2016/01/28/test2.txt', 'w').write('goodbye')
        scraper.create_tarfile('/bin/tar', 'test.tgz', ['2016/01/28/test1.txt',
                                                        '2016/01/28/test2.txt'])
        shutil.rmtree('2016')
        self.assertFalse(os.path.exists('2016'))
        self.assertTrue(os.path.exists('test.tgz'))
        subprocess.check_call(['/bin/tar', 'xfz', 'test.tgz'])
        self.assertTrue(os.path.exists('2016'))
        self.assertEqual(file('2016/01/28/test1.txt').read(), 'hello')
        self.assertEqual(file('2016/01/28/test2.txt').read(), 'goodbye')

    @testfixtures.log_capture()
    def test_create_tarfile_succeeds_on_existing_tarfile(self, log):
        os.makedirs('2016/01/28')
        file('2016/01/28/test1.txt', 'w').write('hello')
        file('2016/01/28/test2.txt', 'w').write('goodbye')
        file('test.tgz', 'w').write('in the way')
        self.assertEqual(file('test.tgz').read(), 'in the way')
        scraper.create_tarfile('/bin/tar', 'test.tgz',
                               ['2016/01/28/test1.txt',
                                '2016/01/28/test2.txt'])
        self.assertIn('WARNING', [x.levelname for x in log.records])

    @testfixtures.log_capture()
    def test_create_tarfile_fails_on_tar_failure(self, log):
        os.makedirs('2016/01/28')
        file('2016/01/28/test1.txt', 'w').write('hello')
        file('2016/01/28/test2.txt', 'w').write('goodbye')
        with self.assertRaises(scraper.NonRecoverableScraperException):
            scraper.create_tarfile('/bin/false', 'test.tgz',
                                   ['2016/01/28/test1.txt',
                                    '2016/01/28/test2.txt'])
        self.assertIn('ERROR', [x.levelname for x in log.records])

    @testfixtures.log_capture()
    def test_create_tarfile_fails_when_file_is_missing(self, log):
        os.makedirs('2016/01/28')
        file('2016/01/28/test1.txt', 'w').write('hello')
        file('2016/01/28/test2.txt', 'w').write('goodbye')
        with self.assertRaises(scraper.NonRecoverableScraperException):
            # Executes successfully, but fails to create the tarfile.
            scraper.create_tarfile('/bin/true', 'test.tgz',
                                   ['2016/01/28/test1.txt',
                                    '2016/01/28/test2.txt'])
        self.assertIn('ERROR', [x.levelname for x in log.records])

    def test_create_tarfiles(self):
        os.makedirs('2016/01/28')
        with scraper.chdir('2016/01/28'):
            file('test1.txt', 'w').write('hello')
            file('test2.txt', 'w').write('goodbye')
            file('test3.txt', 'w').write('compressed')
            subprocess.check_call(['/bin/gzip', 'test3.txt'])
            self.assertFalse(os.path.exists('test3.txt'))
            self.assertTrue(os.path.exists('test3.txt.gz'))
        gen = scraper.create_temporary_tarfiles(
            '/bin/tar', '20160128T000000Z-mlab9-dne04-exper-%04d.tgz',
            self.temp_d, datetime.date(2016, 1, 28), 100000)
        fname, _, count = gen.next()
        self.assertEqual(count, 3)
        self.assertTrue(os.path.isfile(fname))
        shutil.rmtree('2016')
        self.assertFalse(os.path.exists('2016/01/28/test1.txt'))
        self.assertFalse(os.path.exists('2016/01/28/test2.txt'))
        self.assertFalse(os.path.exists('2016/01/28/test3.txt'))
        self.assertFalse(os.path.exists('2016/01/28/test3.txt.gz'))
        subprocess.check_call([
            '/bin/tar', 'xfz',
            '20160128T000000Z-mlab9-dne04-exper-0000.tgz'
        ])
        self.assertTrue(os.path.exists('2016/01/28/test1.txt'))
        self.assertTrue(os.path.exists('2016/01/28/test2.txt'))
        self.assertTrue(os.path.exists('2016/01/28/test3.txt.gz'))
        with self.assertRaises(StopIteration):
            gen.next()

    def test_create_tarfiles_multiple_small_files(self):
        os.makedirs('2016/01/28')
        file('2016/01/28/test1.txt', 'w').write('hello')
        file('2016/01/28/test2.txt', 'w').write('goodbye')
        # By setting the max filesize as 4 bytes, we will end up creating a
        # separate tarfile for each test file.
        gen = scraper.create_temporary_tarfiles(
            '/bin/tar', '20160128T000000Z-mlab9-dne04-exper-%04d.tgz',
            self.temp_d, datetime.date(2016, 1, 28), 4)
        gen.next()
        table1 = subprocess.check_output([
            '/bin/tar', 'tfz',
            '20160128T000000Z-mlab9-dne04-exper-0000.tgz'
        ]).strip()
        self.assertEqual(table1, '2016/01/28/test1.txt')
        gen.next()
        self.assertFalse(os.path.exists(
            '20160128T000000Z-mlab9-dne04-exper-0000.tgz'))
        table2 = subprocess.check_output([
            '/bin/tar', 'tfz',
            '20160128T000000Z-mlab9-dne04-exper-0001.tgz'
        ]).strip()
        self.assertEqual(table2, '2016/01/28/test2.txt')
        with self.assertRaises(StopIteration):
            gen.next()

    def test_create_tarfiles_multiple_small_files_in_dirs(self):
        os.makedirs('2016/01/28/bar')
        file('2016/01/28/bar/test1.txt', 'w').write('hello')
        file('2016/01/28/bar/test2.txt', 'w').write('goodbye')
        # By setting the max filesize as 4 bytes, we will end up creating a
        # separate tarfile for each test file.
        gen = scraper.create_temporary_tarfiles(
            '/bin/tar', '20160128T000000Z-mlab9-dne04-exper-%04d.tgz',
            self.temp_d, datetime.date(2016, 1, 28), 4)
        gen.next()
        table1 = subprocess.check_output([
            '/bin/tar', 'tfz',
            '20160128T000000Z-mlab9-dne04-exper-0000.tgz'
        ]).strip()
        self.assertEqual(table1, '2016/01/28/bar/test1.txt')
        gen.next()
        self.assertFalse(os.path.exists(
            '20160128T000000Z-mlab9-dne04-exper-0000.tgz'))
        table2 = subprocess.check_output([
            '/bin/tar', 'tfz',
            '20160128T000000Z-mlab9-dne04-exper-0001.tgz'
        ]).strip()
        self.assertEqual(table2, '2016/01/28/bar/test2.txt')
        with self.assertRaises(StopIteration):
            gen.next()

    def test_remove_datafiles_all_finished(self):
        os.makedirs('2009/02/28')
        file('2009/02/28/data.txt', 'w').write('test')
        scraper.remove_datafiles(self.temp_d, datetime.date(2009, 2, 28))
        self.assertEqual([], os.listdir(self.temp_d))

    def test_remove_datafiles_not_all_finished(self):
        os.makedirs('2009/02/27')
        file('2009/02/27/data.txt', 'w').write('test')
        os.makedirs('2009/02/28')
        file('2009/02/28/data2.txt', 'w').write('test')
        scraper.remove_datafiles(self.temp_d, datetime.date(2009, 2, 27))
        self.assertEqual(
            ['data2.txt'], os.listdir(os.path.join(self.temp_d, '2009/02/28')))

    @freezegun.freeze_time('2016-01-28 09:45:01 UTC')
    @mock.patch.object(scraper, 'upload_up_to_date')
    def test_initial_upload_empty_disk(self, new_upload):
        scraper.upload_stale_disk({}, None, '.', None)
        self.assertEqual(new_upload.call_count, 0)

    @freezegun.freeze_time('2016-01-28 09:45:01 UTC')
    @mock.patch.object(scraper, 'upload_up_to_date')
    def test_initial_upload_no_safe_dirs(self, new_upload):
        os.makedirs('2016/01/28')
        scraper.upload_stale_disk({}, None, '.', None)
        self.assertEqual(new_upload.call_count, 0)

    @freezegun.freeze_time('2016-01-28 09:45:01 UTC')
    @mock.patch.object(scraper, 'upload_up_to_date')
    @testfixtures.log_capture()
    def test_initial_upload_one_safe_dir(self, new_upload):
        new_upload.return_value = None
        os.makedirs('2016/01/26')
        os.makedirs('2016/01/27')
        self.assertEqual(new_upload.call_count, 0)
        scraper.upload_stale_disk({}, None, '.', None)
        self.assertEqual(new_upload.call_count, 1)
        self.assertEqual(new_upload.call_args[0][-1],
                         datetime.date(2016, 1, 26))

    @freezegun.freeze_time('2016-01-28 09:45:01 UTC')
    @mock.patch.object(scraper, 'upload_up_to_date')
    @testfixtures.log_capture()
    def test_upload_if_allowed(self, new_upload):
        self.assertEqual(new_upload.call_count, 0)
        scraper.upload_if_allowed({}, None, '.', None)
        self.assertEqual(new_upload.call_count, 1)
        self.assertEqual(new_upload.call_args[0][-1],
                         datetime.date(2016, 1, 27))


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
