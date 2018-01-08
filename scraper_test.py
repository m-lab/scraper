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
import time
import unittest

import freezegun
import mock
import testfixtures

# pylint: disable=no-name-in-module
import google.cloud.exceptions as cloud_exceptions
# pylint: enable=no-name-in-module

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

    @testfixtures.log_capture()
    def test_list_rsync_files(self):
        # pylint: disable=line-too-long
        serverfiles = textwrap.dedent("""\
        opening tcp connection to ndt.iupui.mlab1.lga05.measurement-lab.org port 7999
        sending daemon args: --server --sender -vvnlogDtprze.iLsfxC --timeout=300 --bwlimit=10000 . iupui_ndt/  (7 args)
        receiving incremental file list
        delta-transmission enabled
        2017/10/12/.gz is uptodate
        2017/10/12/20171012T22:00:14.809574000Z_66-87-124-30.pools.spcsdns.net:20450.s2c_snaplog.gz is uptodate
        2017/10/12/20171012T22:00:14.809574000Z_66-87-124-30.pools.spcsdns.net:5231.cputime.gz is uptodate
        2017/10/12/20171012T22:00:14.809574000Z_66-87-124-30.pools.spcsdns.net:5231.meta is uptodate
        2017/10/12/20171012T22:11:50.24974000Z_pool-71-187-248-40.nwrknj.fios.verizon.net:58633.cputime.gz is uptodate
        2017/10/12/20171012T22:11:50.24974000Z_pool-71-187-248-40.nwrknj.fios.verizon.net:58633.meta is uptodate
        2017/10/12/20171012T22:11:50.24974000Z_pool-71-187-248-40.nwrknj.fios.verizon.net:58634.s2c_snaplog.gz is uptodate
        [receiver] expand file_list pointer array to 524288 bytes, did move
        [generator] expand file_list pointer array to 524288 bytes, did move
        2017/10/12/ 2017/10/13-08:51:08
        2017/10/12/20171012T22:11:56.252172000Z_72.89.41.162.s2c_ndttrace.gz 2017/10/12-22:12:07
        2017/10/12/20171012T22:11:56.252172000Z_pool-72-89-41-162.nycmny.fios.verizon.net:58176.cputime.gz 2017/10/12-22:12:07
        2017/10/12/20171012T22:11:56.252172000Z_pool-72-89-41-162.nycmny.fios.verizon.net:58176.meta 2017/10/12-22:12:07
        2017/10/12/20171012T22:11:56.252172000Z_pool-72-89-41-162.nycmny.fios.verizon.net:58178.s2c_snaplog.gz 2017/10/12-22:12:07
        2017/10/12/20171012T22:11:56.911421000Z_98.217.9.215.s2c_ndttrace 2017/10/12-22:12:07
        2017/10/12/20171012T22:11:56.911421000Z_98.217.9.215:51934.cputime 2017/10/12-22:12:07
        2017/10/12/20171012T22:11:56.911421000Z_98.217.9.215:51935.s2c_snaplog 2017/10/12-22:12:07
        2017/10/12/20171012T22:12:00.876568000Z_71.187.248.40.c2s_ndttrace.gz 2017/10/12-22:12:24""")
        with tempfile.NamedTemporaryFile() as temp:
            temp.write(serverfiles)
            temp.flush()
            fake_process = subprocess.Popen(['/bin/cat', temp.name],
                                            stdout=subprocess.PIPE)
            with mock.patch.object(subprocess, 'Popen') as mock_subprocess:
                mock_subprocess.return_value = fake_process
                files = scraper.list_rsync_files(
                    '/usr/bin/timeout', '/usr/bin/rsync', 'localhost', '/tmp')
        self.assertSetEqual(
            set([
                scraper.RemoteFile(
                    '2017/10/12/20171012T22:11:56.252172000Z_72.89.41.162.s2c_ndttrace.gz',
                    datetime.datetime(2017, 10, 12, 22, 12, 7)),
                scraper.RemoteFile(
                    '2017/10/12/20171012T22:11:56.252172000Z_pool-72-89-41-162.nycmny.fios.verizon.net:58176.cputime.gz',
                    datetime.datetime(2017, 10, 12, 22, 12, 7)),
                scraper.RemoteFile(
                    '2017/10/12/20171012T22:11:56.252172000Z_pool-72-89-41-162.nycmny.fios.verizon.net:58176.meta',
                    datetime.datetime(2017, 10, 12, 22, 12, 7)),
                scraper.RemoteFile(
                    '2017/10/12/20171012T22:11:56.252172000Z_pool-72-89-41-162.nycmny.fios.verizon.net:58178.s2c_snaplog.gz',
                    datetime.datetime(2017, 10, 12, 22, 12, 7)),
                scraper.RemoteFile(
                    '2017/10/12/20171012T22:11:56.911421000Z_98.217.9.215.s2c_ndttrace',
                    datetime.datetime(2017, 10, 12, 22, 12, 7)),
                scraper.RemoteFile(
                    '2017/10/12/20171012T22:11:56.911421000Z_98.217.9.215:51934.cputime',
                    datetime.datetime(2017, 10, 12, 22, 12, 7)),
                scraper.RemoteFile(
                    '2017/10/12/20171012T22:11:56.911421000Z_98.217.9.215:51935.s2c_snaplog',
                    datetime.datetime(2017, 10, 12, 22, 12, 7)),
                scraper.RemoteFile(
                    '2017/10/12/20171012T22:12:00.876568000Z_71.187.248.40.c2s_ndttrace.gz',
                    datetime.datetime(2017, 10, 12, 22, 12, 24))]),
            set(files))
        # pylint: enable=line-too-long

    @mock.patch.object(subprocess, 'Popen')
    @testfixtures.log_capture()
    def test_list_rsync_files_returns_24(self, patched_subprocess):
        # pylint: disable=line-too-long
        serverfiles = textwrap.dedent("""\
            .
            2016/
            2016/01/ 2016/01/06-05:12:07
            2016/01/06/ 2016/01/06-05:12:07
            2016/01/06/.gz 2016/01/06-05:12:07
            2016/01/06/20160106T05:43:32.741066000Z_:0.cputime.gz 2016/01/06-05:43:32
            2016/01/06/20160106T05:43:32.741066000Z_:0.meta 2016/01/06-05:43:32
            2016/01/06/20160106T18:07:33.122784000Z_:0.cputime.gz 2016/01/06-18:07:33
            BADBADBAD
            2016/01/06/20160106T18:07:33.122784000Z_:0.meta 2016/01/06-18:07:33
            2016/01/06/20160106T22:31:57.229531000Z_:0.cputime.gz 2016/01/06-22:31:57""")
        mock_process = mock.Mock()
        mock_process.returncode = 24
        patched_subprocess.return_value = mock_process
        mock_process.stdout = serverfiles.splitlines()
        files = set(scraper.list_rsync_files(
            '/usr/bin/timeout', '/usr/bin/rsync', 'localhost', ''))
        self.assertSetEqual(
            set([
                scraper.RemoteFile('2016/01/06/.gz',
                                   datetime.datetime(2016, 1, 6, 5, 12, 7)),
                scraper.RemoteFile('2016/01/06/20160106T05:43:32.741066000Z_:0.cputime.gz',
                                   datetime.datetime(2016, 1, 6, 5, 43, 32)),
                scraper.RemoteFile('2016/01/06/20160106T05:43:32.741066000Z_:0.meta',
                                   datetime.datetime(2016, 1, 6, 5, 43, 32)),
                scraper.RemoteFile('2016/01/06/20160106T18:07:33.122784000Z_:0.cputime.gz',
                                   datetime.datetime(2016, 1, 6, 18, 7, 33)),
                scraper.RemoteFile('2016/01/06/20160106T18:07:33.122784000Z_:0.meta',
                                   datetime.datetime(2016, 1, 6, 18, 7, 33)),
                scraper.RemoteFile('2016/01/06/20160106T22:31:57.229531000Z_:0.cputime.gz',
                                   datetime.datetime(2016, 1, 6, 22, 31, 57))]),
            files)
        # pylint: enable=line-too-long

    @mock.patch.object(subprocess, 'Popen')
    @testfixtures.log_capture()
    def test_list_rsync_files_throws_on_failure(self, patched_subprocess, log):
        mock_process = mock.Mock()
        mock_process.returncode = 1
        mock_process.stdout = []
        patched_subprocess.return_value = mock_process
        with self.assertRaises(scraper.RecoverableScraperException):
            scraper.list_rsync_files(
                '/usr/bin/timeout', '/usr/bin/rsync', 'localhost', '')
        self.assertIn('ERROR', [x.levelname for x in log.records])

    @testfixtures.log_capture()
    def test_list_rsync_files_fails(self, log):
        with self.assertRaises(scraper.RecoverableScraperException):
            scraper.list_rsync_files(
                '/usr/bin/timeout', '/bin/false', 'localhost', '')
        self.assertIn('ERROR', [x.levelname for x in log.records])

    @testfixtures.log_capture()
    def test_download_files_fails_and_dies(self, log):
        with self.assertRaises(scraper.RecoverableScraperException):
            scraper.download_files('/usr/bin/timeout', '/bin/false',
                                   'localhost/',
                                   [scraper.RemoteFile('2016/10/26/DNE1', 0),
                                    scraper.RemoteFile('2016/10/26/DNE2', 0)],
                                   '/tmp')
        self.assertIn('ERROR', [x.levelname for x in log.records])

    @testfixtures.log_capture()
    def test_download_files_with_empty_does_nothing(self, _log):
        # If the next line doesn't raise SystemExit then the test passes
        scraper.download_files(
            '/usr/bin/timeout', '/bin/false', 'localhost/', [], '/tmp')

    @mock.patch.object(subprocess, 'call')
    def test_download_files(self, patched_call):
        files_to_download = [
            scraper.RemoteFile('2016/10/26/DNE1', 0),
            scraper.RemoteFile('2016/10/26/DNE2', 0)]

        def verify_contents(args):
            # Verify that the third-to-last argument to check_call is a filename
            # that contains the right data (specifically, the filenames).  This
            # test needs to be kept in sync with the order of command-line
            # arguments passed to the rsync call.
            file_with_filenames = args[-3]
            files_downloaded = file(file_with_filenames).read().split('\0')
            self.assertEqual(files_downloaded,
                             [x.filename for x in files_to_download])
            return 0

        patched_call.side_effect = verify_contents
        self.assertEqual(patched_call.call_count, 0)
        scraper.download_files('/usr/bin/timeout', '/bin/true', 'localhost/',
                               files_to_download, '/tmp')
        self.assertEqual(patched_call.call_count, 1)

    @mock.patch.object(subprocess, 'call')
    def test_download_files_breaks_up_long_file_list(self, patched_call):
        files_to_download = [scraper.RemoteFile('2016/10/26/DNE.%d' % i, 0)
                             for i in range(100070)]
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
        scraper.download_files('/usr/bin/timeout', '/bin/true', 'localhost/',
                               files_to_download, '/tmp')
        self.assertEqual(set(x.filename for x in files_to_download),
                         set(files_downloaded))
        self.assertEqual(patched_call.call_count, 101)

    @freezegun.freeze_time('2016-01-28 09:45:01 UTC')
    def test_new_archived_date_after_8am(self):
        self.assertEqual(scraper.must_upload_up_to(),
                         datetime.datetime(2016, 1, 27, 23, 59, 59))

    @freezegun.freeze_time('2016-01-28 07:43:16 UTC')
    def test_new_archived_date_before_8am(self):
        self.assertEqual(scraper.must_upload_up_to(),
                         datetime.datetime(2016, 1, 26, 23, 59, 59))

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
        last_archived_date = status.get_last_archived_mtime()
        self.assertEqual(last_archived_date,
                         datetime.datetime(2009, 1, 1, 0, 0, 0))

    @mock.patch.object(scraper.SyncStatus, 'get_data')
    def test_get_last_archived_date_from_status_no_date(self, patched_get):
        patched_get.return_value = dict(irrelevant='monkey')
        status = scraper.SyncStatus(None, None)
        last_archived_date = status.get_last_archived_mtime()
        self.assertEqual(last_archived_date,
                         datetime.datetime(2009, 1, 1, 0, 0, 0))

    @mock.patch.object(scraper.SyncStatus, 'get_data')
    @testfixtures.log_capture()
    def test_get_last_archived_date_bad_date(self, patched_get, log):
        status = scraper.SyncStatus(None, None)
        with self.assertRaises(scraper.NonRecoverableScraperException):
            patched_get.return_value = dict(
                maxrawfilemtimearchived='monkey')
            status.get_last_archived_mtime()
        self.assertIn('ERROR', [x.levelname for x in log.records])

    @mock.patch.object(scraper.SyncStatus, 'get_data')
    def test_get_last_archived_date_empty_date(self, patched_get):
        status = scraper.SyncStatus(None, None)
        patched_get.return_value = dict(lastsuccessfulcollection='')
        default_date = datetime.datetime(1970, 1, 1, 23, 59, 59)
        self.assertEqual(status.get_last_archived_mtime(default_date),
                         default_date)

    @mock.patch.object(scraper.SyncStatus, 'get_data')
    def test_get_last_archived_date(self, patched_get):
        status = scraper.SyncStatus(None, None)
        timestamp = datetime.datetime(2010, 11, 2, 3, 44, 55)
        mtime = (timestamp - datetime.datetime(1970, 1, 1)).total_seconds()
        patched_get.return_value = dict(maxrawfilemtimearchived=mtime)
        last_archived_date = status.get_last_archived_mtime()
        self.assertEqual(last_archived_date, timestamp)

    @mock.patch.object(scraper.SyncStatus, 'update_data')
    def test_update_last_archived_date(self, patched_update):
        status = scraper.SyncStatus(None, None)
        status.update_last_archived_date(datetime.date(2012, 2, 29))
        self.assertEqual(patched_update.call_count, 1)
        self.assertTrue(u'obsolete' in patched_update.call_args[0])

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
        self.assertEqual(patched_update_data.call_args,
                         [('errorsincelastsuccessful', 'msg')])
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
        self.assertEqual(patched_update_data.call_args,
                         [('lastcollectionattempt', 'x2016-01-28-07:43')])
        self.assertEqual(type(patched_update_data.call_args[0][1]),
                         unicode)

    @mock.patch.object(scraper.SyncStatus, 'update_data')
    def test_update_mtime(self, patched_update_data):
        status = scraper.SyncStatus(None, None)
        status.update_mtime(7)
        self.assertEqual(patched_update_data.call_args,
                         [('maxrawfilemtimearchived', 7)])

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

    def test_day_of_week(self):
        self.assertEqual(scraper.day_of_week(datetime.date(2017, 6, 15)),
                         'Thursday')
        self.assertEqual(scraper.day_of_week(datetime.date(2017, 6, 16)),
                         'Friday')


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
            new_mtime = scraper.datetime_to_epoch(
                datetime.datetime(2016, 1, 28, 1, 1, 1))
            for fname in ('test1.txt', 'test2.txt', 'test3.txt.gz'):
                os.utime(fname, (new_mtime, new_mtime))

        template = scraper.TarfileTemplate(self.temp_d, 'mlab9', 'dne04',
                                           'exper')
        gen = scraper.create_temporary_tarfiles(
            '/bin/tar', template, self.temp_d,
            datetime.datetime(2016, 1, 28, 0, 0, 0),
            datetime.datetime(2016, 1, 28, 23, 59, 59),
            100000)
        fname, _, _, count = gen.next()
        self.assertEqual(count, 3)
        self.assertTrue(os.path.isfile(fname))
        shutil.rmtree('2016')
        self.assertFalse(os.path.exists('2016/01/28/test1.txt'))
        self.assertFalse(os.path.exists('2016/01/28/test2.txt'))
        self.assertFalse(os.path.exists('2016/01/28/test3.txt'))
        self.assertFalse(os.path.exists('2016/01/28/test3.txt.gz'))
        subprocess.check_call([
            '/bin/tar', 'xfz',
            '20160128T010101Z-mlab9-dne04-exper-0000.tgz'
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
        new_mtime = scraper.datetime_to_epoch(
            datetime.datetime(2016, 1, 28, 1, 1, 1))
        os.utime('2016/01/28/test1.txt', (new_mtime, new_mtime))
        new_mtime = scraper.datetime_to_epoch(
            datetime.datetime(2016, 1, 28, 1, 1, 2))
        os.utime('2016/01/28/test2.txt', (new_mtime, new_mtime))
        # By setting the max filesize as 4 bytes, we will end up creating a
        # separate tarfile for each test file.
        template = scraper.TarfileTemplate(self.temp_d, 'mlab9', 'dne04',
                                           'exper')
        gen = scraper.create_temporary_tarfiles(
            '/bin/tar', template, self.temp_d,
            datetime.datetime(2016, 1, 28, 0, 0, 0),
            datetime.datetime(2016, 1, 28, 23, 59, 59),
            4)
        gen.next()
        table1 = subprocess.check_output([
            '/bin/tar', 'tfz',
            '20160128T010101Z-mlab9-dne04-exper-0000.tgz'
        ]).strip()
        self.assertEqual(table1, '2016/01/28/test1.txt')
        gen.next()
        self.assertFalse(os.path.exists(
            '20160128T010101Z-mlab9-dne04-exper-0000.tgz'))
        table2 = subprocess.check_output([
            '/bin/tar', 'tfz',
            '20160128T010102Z-mlab9-dne04-exper-0000.tgz'
        ]).strip()
        self.assertEqual(table2, '2016/01/28/test2.txt')
        with self.assertRaises(StopIteration):
            gen.next()

    def test_create_tarfiles_multiple_small_files_in_dirs(self):
        os.makedirs('2016/01/28/bar')
        file('2016/01/28/bar/test1.txt', 'w').write('hello')
        file('2016/01/28/bar/test2.txt', 'w').write('goodbye')
        new_mtime = scraper.datetime_to_epoch(
            datetime.datetime(2016, 1, 28, 1, 1, 1))
        os.utime('2016/01/28/bar/test1.txt', (new_mtime, new_mtime))
        new_mtime = scraper.datetime_to_epoch(
            datetime.datetime(2016, 1, 28, 1, 1, 2))
        os.utime('2016/01/28/bar/test2.txt', (new_mtime, new_mtime))
        # By setting the max filesize as 4 bytes, we will end up creating a
        # separate tarfile for each test file.
        template = scraper.TarfileTemplate(self.temp_d, 'mlab9', 'dne04',
                                           'exper')
        gen = scraper.create_temporary_tarfiles(
            '/bin/tar', template, self.temp_d,
            datetime.datetime(2016, 1, 28, 0, 0, 0),
            datetime.datetime(2016, 1, 28, 23, 59, 59),
            4)
        gen.next()
        table1 = subprocess.check_output([
            '/bin/tar', 'tfz',
            '20160128T010101Z-mlab9-dne04-exper-0000.tgz'
        ]).strip()
        self.assertEqual(table1, '2016/01/28/bar/test1.txt')
        gen.next()
        self.assertFalse(os.path.exists(
            '20160128T010101Z-mlab9-dne04-exper-0000.tgz'))
        table2 = subprocess.check_output([
            '/bin/tar', 'tfz',
            '20160128T010102Z-mlab9-dne04-exper-0000.tgz'
        ]).strip()
        self.assertEqual(table2, '2016/01/28/bar/test2.txt')
        with self.assertRaises(StopIteration):
            gen.next()

    def test_delete_datafiles_up_to_all_files_gone(self):
        os.makedirs('2009/02/28')
        timestamp = scraper.datetime_to_epoch(
            datetime.datetime(2009, 2, 28, 1, 1, 1))
        file('2009/02/28/data.txt', 'w').write('test')
        os.utime('2009/02/28/data.txt', (timestamp, timestamp))

        max_mtime = scraper.datetime_to_epoch(
            datetime.datetime(2009, 3, 1, 0, 0, 0))
        scraper.delete_local_datafiles_up_to(self.temp_d, max_mtime)
        self.assertEqual([], os.listdir(self.temp_d))

    def test_remove_datafiles_not_all_finished(self):
        # Old file
        os.makedirs('2009/02/27')
        timestamp = scraper.datetime_to_epoch(
            datetime.datetime(2009, 2, 27, 1, 1, 1))
        file('2009/02/27/data.txt', 'w').write('test')
        os.utime('2009/02/27/data.txt', (timestamp, timestamp))

        # Newer file
        os.makedirs('2009/02/28')
        timestamp = scraper.datetime_to_epoch(
            datetime.datetime(2009, 2, 28, 1, 1, 1))
        file('2009/02/28/data2.txt', 'w').write('test')
        os.utime('2009/02/28/data2.txt', (timestamp, timestamp))

        max_timestamp = scraper.datetime_to_epoch(
            datetime.datetime(2009, 2, 28, 0, 0, 0))
        scraper.delete_local_datafiles_up_to(self.temp_d, max_timestamp)
        self.assertEqual(
            ['data2.txt'], os.listdir(os.path.join(self.temp_d, '2009/02/28')))

    @freezegun.freeze_time('2016-01-28 09:45:01 UTC')
    @mock.patch.object(scraper, 'upload_up_to_date')
    def test_initial_upload_empty_disk(self, new_upload):
        mock_status = mock.MagicMock()
        mock_status.get_last_archived_mtime.return_value = datetime.datetime(
            2016, 1, 27, 9, 9, 9)
        mock_args = mock.Mock()
        mock_args.data_wait_time = datetime.timedelta(hours=1)
        scraper.upload_stale_disk(mock_args, mock_status, '.', None)
        self.assertEqual(new_upload.call_count, 0)

    @freezegun.freeze_time('2016-01-28 09:45:01 UTC')
    @mock.patch.object(scraper, 'upload_up_to_date')
    @testfixtures.log_capture()
    def test_initial_upload_with_enough_data(self, new_upload):
        new_upload.return_value = None

        os.makedirs('2016/01/26')
        open('2016/01/26/testdata.txt', 'w').write('x' * 2048)
        tstamp = int(time.time()) - 48 * 60 * 60
        os.utime('2016/01/26/testdata.txt', (tstamp, tstamp))

        # Only the younger of the files should be uploaded.  The most recent
        # mtime is used as evidence of the last time rsync was run, and then the
        # system backs up for the max time between rsync runs before picking a
        # set of data to upload.
        os.makedirs('2016/01/27')
        open('2016/01/27/testdata.txt', 'w').write('x' * 2048)
        tstamp = int(time.time()) - 24 * 60 * 60
        os.utime('2016/01/27/testdata.txt', (tstamp, tstamp))

        mock_status = mock.MagicMock()
        mock_status.get_last_archived_mtime.return_value = datetime.datetime(
            2016, 1, 25, 9, 9, 9)
        mock_args = mock.Mock()
        mock_args.data_wait_time = datetime.timedelta(hours=1)
        mock_args.data_buffer_threshold = 1000

        self.assertEqual(new_upload.call_count, 0)
        scraper.upload_stale_disk(mock_args, mock_status, '.', None)
        self.assertEqual(new_upload.call_count, 1)
        self.assertLess(new_upload.call_args[0][-1],
                        datetime.datetime(2016, 1, 27, 9, 45, 1))

    @freezegun.freeze_time('2016-01-28 09:45:01 UTC')
    @mock.patch.object(scraper, 'upload_up_to_date')
    @testfixtures.log_capture()
    def test_upload_if_allowed(self, new_upload):
        status = mock.Mock()
        status.get_last_archived_mtime.return_value = datetime.datetime(
            2016, 1, 26, 23, 59, 59)
        self.assertEqual(new_upload.call_count, 0)
        args = mock.Mock()
        args.data_wait_time = datetime.timedelta(hours=1)
        scraper.upload_if_allowed(args, status, '.', None)
        self.assertEqual(new_upload.call_count, 1)
        self.assertEqual(new_upload.call_args[0][-1],
                         datetime.datetime(2016, 1, 27, 23, 59, 59))


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
