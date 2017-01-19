#!/usr/bin/env python
# Copyright 2016 Scraper Authors
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
import unittest

import fasteners
import freezegun
import mock
import scraper


class TestScraper(unittest.TestCase):

    def test_file_locking(self):
        try:
            temp_d = tempfile.mkdtemp()
            lockfile = os.path.join(temp_d, 'testlockfile')
            scraper.acquire_lock_or_die(lockfile)
            self.assertTrue(os.path.exists(lockfile))
        finally:
            shutil.rmtree(temp_d)

    @mock.patch.object(fasteners.InterProcessLock, 'acquire',
                       return_value=False)
    def test_file_locking_failure_causes_exit(self, _patched_acquire):
        try:
            temp_d = tempfile.mkdtemp()
            lockfile = os.path.join(temp_d, 'testlockfile')
            with self.assertRaises(SystemExit):
                scraper.acquire_lock_or_die(lockfile)
        finally:
            shutil.rmtree(temp_d)

    def test_args(self):
        rsync_host = 'mlab1.dne0t.measurement-lab.org'
        lockfile_dir = '/tmp/shouldnotexist/'
        rsync_module = 'ndt'
        data_dir = '/tmp/bigplaceforbackup'
        rsync_binary = '/usr/bin/rsync'
        spreadsheet = '1234567890abcdef'
        rsync_port = 1234
        max_uncompressed_size = 1024
        args = scraper.parse_cmdline([
            '--rsync_host', rsync_host, '--lockfile_dir', lockfile_dir,
            '--rsync_module', rsync_module, '--data_dir', data_dir,
            '--rsync_binary', rsync_binary, '--rsync_port', str(rsync_port),
            '--spreadsheet', spreadsheet, '--max_uncompressed_size',
            str(max_uncompressed_size)
        ])
        self.assertEqual(args.rsync_host, rsync_host)
        self.assertEqual(args.lockfile_dir, lockfile_dir)
        self.assertEqual(args.rsync_module, rsync_module)
        self.assertEqual(args.data_dir, data_dir)
        self.assertEqual(args.rsync_binary, rsync_binary)
        self.assertEqual(args.rsync_port, rsync_port)
        self.assertEqual(args.spreadsheet, spreadsheet)
        self.assertEqual(args.max_uncompressed_size, max_uncompressed_size)
        args = scraper.parse_cmdline([
            '--rsync_host', rsync_host, '--lockfile_dir', lockfile_dir,
            '--rsync_module', rsync_module, '--data_dir', data_dir,
            '--spreadsheet', spreadsheet
        ])
        self.assertEqual(args.rsync_binary, '/usr/bin/rsync')
        self.assertEqual(args.rsync_port, 7999)
        self.assertEqual(args.max_uncompressed_size, 1000000000)

    def test_args_help(self):
        with self.assertRaises(SystemExit):
            scraper.parse_cmdline(['-h'])

    @mock.patch.object(subprocess, 'check_output')
    def test_list_rsync_files(self, patched_subprocess):
        # pylint: disable=line-too-long
        serverfiles = """\
drwxr-xr-x          4,096 2016/01/06 05:43:33 .
drwxr-xr-x          4,096 2016/10/01 00:06:59 2016
drwxr-xr-x          4,096 2016/01/15 01:03:29 2016/01
drwxr-xr-x          4,096 2016/01/06 22:32:01 2016/01/06
-rw-r--r--              0 2016/01/06 22:32:01 2016/01/06/.gz
-rw-r--r--            103 2016/01/06 05:43:36 2016/01/06/20160106T05:43:32.741066000Z_:0.cputime.gz
-rw-r--r--            716 2016/01/06 05:43:36 2016/01/06/20160106T05:43:32.741066000Z_:0.meta
-rw-r--r--            101 2016/01/06 18:07:37 2016/01/06/20160106T18:07:33.122784000Z_:0.cputime.gz
BADBADBAD
-rw-r--r--            716 2016/01/06 18:07:37 2016/01/06/20160106T18:07:33.122784000Z_:0.meta
-rw-r--r--            103 2016/01/06 22:32:01 2016/01/06/20160106T22:31:57.229531000Z_:0.cputime.gz"""
        # pylint: enable=line-too-long
        patched_subprocess.return_value = serverfiles
        files = scraper.list_rsync_files('/usr/bin/rsync', 'localhost')
        self.assertEqual([
            '2016/01/06/.gz',
            '2016/01/06/20160106T05:43:32.741066000Z_:0.cputime.gz',
            '2016/01/06/20160106T05:43:32.741066000Z_:0.meta',
            '2016/01/06/20160106T18:07:33.122784000Z_:0.cputime.gz',
            '2016/01/06/20160106T18:07:33.122784000Z_:0.meta',
            '2016/01/06/20160106T22:31:57.229531000Z_:0.cputime.gz'
        ], files)

    def test_list_rsync_files_fails(self):
        with self.assertRaises(SystemExit):
            scraper.list_rsync_files('/bin/false', 'localhost')

    def test_remove_older_files(self):
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
        # pylint: enable=line-too-long

    def test_download_files_fails_and_dies(self):
        with self.assertRaises(SystemExit):
            scraper.download_files('/bin/false', 'localhost/',
                                   ['2016/10/26/DNE1', '2016/10/26/DNE2'],
                                   '/tmp')

    def test_download_files_with_empty_does_nothing(self):
        # If the next line doesn't raise SystemExit then the test passes
        scraper.download_files('/bin/false', 'localhost/', [], '/tmp')

    @mock.patch.object(subprocess, 'check_call')
    def test_download_files(self, patched_check_call):
        files = ['2016/10/26/DNE1', '2016/10/26/DNE2']

        def verify_contents(args):
            self.assertEqual(files,
                             [x.strip() for x in file(args[2]).readlines()])

        patched_check_call.side_effect = verify_contents
        scraper.download_files('/bin/true', 'localhost/',
                               ['2016/10/26/DNE1', '2016/10/26/DNE2'], '/tmp')
        self.assertTrue(patched_check_call.called)
        self.assertEqual(patched_check_call.call_count, 1)

    @freezegun.freeze_time('2016-01-28 09:45:01 UTC')
    def test_high_water_mark_after_8am(self):
        self.assertEqual(scraper.max_new_high_water_mark(),
                         datetime.date(2016, 1, 27))

    @freezegun.freeze_time('2016-01-28 07:43:16 UTC')
    def test_high_water_mark_before_8am(self):
        self.assertEqual(scraper.max_new_high_water_mark(),
                         datetime.date(2016, 1, 26))

    def test_find_all_days_to_upload_empty_okay(self):
        try:
            temp_d = tempfile.mkdtemp()
            date = datetime.date(2016, 7, 6)
            to_upload = list(scraper.find_all_days_to_upload(temp_d, date))
            self.assertEqual(to_upload, [])
        finally:
            shutil.rmtree(temp_d)

    def test_find_all_days_to_upload(self):
        try:
            temp_d = tempfile.mkdtemp()
            date = datetime.date(2016, 7, 6)
            with scraper.chdir(temp_d):
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
            to_upload = list(scraper.find_all_days_to_upload(temp_d, date))
            self.assertEqual(to_upload, [
                datetime.date(2015, 10, 31), datetime.date(2016, 7, 5),
                datetime.date(2016, 7, 6)
            ])
        finally:
            shutil.rmtree(temp_d)

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

    def test_create_tarfile(self):
        try:
            temp_d = tempfile.mkdtemp()
            with scraper.chdir(temp_d):
                os.makedirs('2016/01/28')
                file('2016/01/28/test1.txt', 'w').write('hello')
                file('2016/01/28/test2.txt', 'w').write('goodbye')
                scraper.create_tarfile(
                    '/bin/tar', 'test.tgz',
                    ['2016/01/28/test1.txt', '2016/01/28/test2.txt'])
                shutil.rmtree('2016')
                self.assertFalse(os.path.exists('2016'))
                self.assertTrue(os.path.exists('test.tgz'))
                subprocess.check_call(['/bin/tar', 'xfz', 'test.tgz'])
                self.assertTrue(os.path.exists('2016'))
                self.assertEqual(file('2016/01/28/test1.txt').read(), 'hello')
                self.assertEqual(file('2016/01/28/test2.txt').read(), 'goodbye')
        finally:
            shutil.rmtree(temp_d)

    def test_create_tarfile_fails_on_existing_tarfile(self):
        try:
            temp_d = tempfile.mkdtemp()
            with scraper.chdir(temp_d):
                os.makedirs('2016/01/28')
                file('2016/01/28/test1.txt', 'w').write('hello')
                file('2016/01/28/test2.txt', 'w').write('goodbye')
                file('test.tgz', 'w').write('in the way')
                self.assertEqual(file('test.tgz').read(), 'in the way')
                with self.assertRaises(SystemExit):
                    scraper.create_tarfile(
                        '/bin/tar', 'test.tgz',
                        ['2016/01/28/test1.txt', '2016/01/28/test2.txt'])
        finally:
            shutil.rmtree(temp_d)

    def test_create_tarfile_fails_on_tar_failure(self):
        try:
            temp_d = tempfile.mkdtemp()
            with scraper.chdir(temp_d):
                os.makedirs('2016/01/28')
                file('2016/01/28/test1.txt', 'w').write('hello')
                file('2016/01/28/test2.txt', 'w').write('goodbye')
                with self.assertRaises(SystemExit):
                    scraper.create_tarfile(
                        '/bin/false', 'test.tgz',
                        ['2016/01/28/test1.txt', '2016/01/28/test2.txt'])
        finally:
            shutil.rmtree(temp_d)

    def test_create_tarfile_fails_when_file_is_missing(self):
        try:
            temp_d = tempfile.mkdtemp()
            with scraper.chdir(temp_d):
                os.makedirs('2016/01/28')
                file('2016/01/28/test1.txt', 'w').write('hello')
                file('2016/01/28/test2.txt', 'w').write('goodbye')
                with self.assertRaises(SystemExit):
                    # Executes successfully, but fails to create the tarfile.
                    scraper.create_tarfile(
                        '/bin/true', 'test.tgz',
                        ['2016/01/28/test1.txt', '2016/01/28/test2.txt'])
        finally:
            shutil.rmtree(temp_d)

    def test_create_tarfiles(self):
        try:
            temp_d = tempfile.mkdtemp()
            with scraper.chdir(temp_d):
                os.makedirs('2016/01/28')
                with scraper.chdir('2016/01/28'):
                    file('test1.txt', 'w').write('hello')
                    file('test2.txt', 'w').write('goodbye')
                    file('test3.txt', 'w').write('compressed')
                    subprocess.check_call(['/bin/gzip', 'test3.txt'])
                    self.assertFalse(os.path.exists('test3.txt'))
                    self.assertTrue(os.path.exists('test3.txt.gz'))
            files = [f for f, _t in scraper.create_temporary_tarfiles(
                '/bin/tar', '/bin/gunzip', temp_d, datetime.date(2016, 1, 28),
                'mlab9.dne04.measurement-lab.org', 'exper', 100000)]
            self.assertEqual(files,
                             ['20160128T000000Z-mlab9-dne04-exper-0000.tgz'])
            with scraper.chdir(temp_d):
                gen = scraper.create_temporary_tarfiles(
                    '/bin/tar', '/bin/gunzip', temp_d,
                    datetime.date(2016, 1, 28),
                    'mlab9.dne04.measurement-lab.org', 'exper', 100000)
                fname, _ = gen.next()
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
                self.assertTrue(os.path.exists('2016/01/28/test3.txt'))
                with self.assertRaises(StopIteration):
                    gen.next()
        finally:
            shutil.rmtree(temp_d)

    def test_create_tarfiles_multiple_small_files(self):
        try:
            temp_d = tempfile.mkdtemp()
            with scraper.chdir(temp_d):
                os.makedirs('2016/01/28')
                file('2016/01/28/test1.txt', 'w').write('hello')
                file('2016/01/28/test2.txt', 'w').write('goodbye')
            # By setting the max filesize as 4 bytes, we will end up creating a
            # separate tarfile for each test file.
            files = [f for f, _t in scraper.create_temporary_tarfiles(
                '/bin/tar', '/bin/gunzip', temp_d, datetime.date(2016, 1, 28),
                'mlab9.dne04.measurement-lab.org', 'exper', 4)]
            self.assertEqual(files, [
                '20160128T000000Z-mlab9-dne04-exper-0000.tgz',
                '20160128T000000Z-mlab9-dne04-exper-0001.tgz'
            ])
            with scraper.chdir(temp_d):
                gen = scraper.create_temporary_tarfiles(
                    '/bin/tar', '/bin/gunzip', temp_d,
                    datetime.date(2016, 1, 28),
                    'mlab9.dne04.measurement-lab.org', 'exper', 4)
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
        finally:
            shutil.rmtree(temp_d)

    @mock.patch.object(scraper.Spreadsheet, 'get_data')
    def test_get_progress_from_spreadsheet_default(self, patched_get):
        patched_get.return_value = [
            [u'dropboxrsyncaddress', u'lastsuccessfulcollection'],
            [1, 2]]
        sheet = scraper.Spreadsheet(None, None)
        high_water_mark = sheet.get_progress('not in sheet')
        self.assertEqual(high_water_mark, datetime.date(2009, 1, 1))

    @mock.patch.object(scraper.Spreadsheet, 'get_data', return_value=[])
    def test_get_progress_from_empty_spreadsheet(self, _patched_get):
        sheet = scraper.Spreadsheet(None, None)
        with self.assertRaises(SystemExit):
            sheet.get_progress('barf')

    @mock.patch.object(scraper.Spreadsheet, 'get_data')
    def test_get_progress_from_spreadsheet_empty_date(self, patched_get):
        sheet = scraper.Spreadsheet(None, None)
        with self.assertRaises(SystemExit):
            rsync_url = u'rsync://localhost:1234/ndt'
            patched_get.return_value = [
                [u'dropboxrsyncaddress', u'lastsuccessfulcollection'],
                [u'not this one', u'x2009-12-03'],
                [rsync_url, u''],
                [u'not this one either', u'x2009-09-09']]
            sheet.get_progress(rsync_url)

    @mock.patch.object(scraper.Spreadsheet, 'get_data')
    def test_get_progress_from_spreadsheet_bad_date(self, patched_get):
        sheet = scraper.Spreadsheet(None, None)
        with self.assertRaises(SystemExit):
            rsync_url = u'rsync://localhost:1234/ndt'
            patched_get.return_value = [
                [u'dropboxrsyncaddress', u'lastsuccessfulcollection'],
                [u'not this one', u'x2009-12-03'],
                [rsync_url, u'2009-13-10'],
                [u'not this one either', u'x2009-09-09']]
            sheet.get_progress(rsync_url)

    @mock.patch.object(scraper.Spreadsheet, 'get_data')
    def test_get_progress_from_spreadsheet(self, patched_get):
        sheet = scraper.Spreadsheet(None, None)
        rsync_url = u'rsync://localhost:1234/ndt'
        patched_get.return_value = [
            [u'dropboxrsyncaddress', u'lastsuccessfulcollection'],
            [u'not this one', u'x2009-12-03'],
            [rsync_url, u'x2010-11-02'],
            [u'not this one either', u'x2009-09-09']]
        high_water_mark = sheet.get_progress(rsync_url)
        self.assertEqual(high_water_mark, datetime.date(2010, 11, 2))

    @mock.patch.object(scraper.Spreadsheet, 'update_data')
    def test_high_water_mark(self, patched_update):
        sheet = scraper.Spreadsheet(None, None)
        sheet.update_high_water_mark('rsync://localhost:7999/test',
                                     datetime.date(2012, 2, 29))
        patched_update.assert_called_once()
        self.assertTrue('x2012-02-29' in patched_update.call_args[0])

    def test_remove_datafiles_all_finished(self):
        try:
            temp_d = tempfile.mkdtemp()
            with scraper.chdir(temp_d):
                os.makedirs('2009/02/28')
                file('2009/02/28/data.txt', 'w').write('test')
            scraper.remove_datafiles(temp_d, datetime.date(2009, 2, 28))
            self.assertEqual([], os.listdir(temp_d))
        finally:
            shutil.rmtree(temp_d)

    def test_remove_datafiles_not_all_finished(self):
        try:
            temp_d = tempfile.mkdtemp()
            with scraper.chdir(temp_d):
                os.makedirs('2009/02/27')
                file('2009/02/27/data.txt', 'w').write('test')
                os.makedirs('2009/02/28')
                file('2009/02/28/data2.txt', 'w').write('test')
            scraper.remove_datafiles(temp_d, datetime.date(2009, 2, 27))
            self.assertEqual(
                ['data2.txt'], os.listdir(os.path.join(temp_d, '2009/02/28')))
        finally:
            shutil.rmtree(temp_d)

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

    @mock.patch.object(scraper.Spreadsheet, 'update_data')
    def test_update_debug_msg(self, patched_update_data):
        sheet = scraper.Spreadsheet(None, None)
        sheet.update_debug_message('rsync://localhost/ndt', 'msg')
        patched_update_data.assert_called_once_with(
            'rsync://localhost/ndt', 'errorsincelastsuccessful', 'msg')

    @freezegun.freeze_time('2016-01-28 07:43:16 UTC')
    @mock.patch.object(scraper.Spreadsheet, 'update_data')
    def test_update_last_collection(self, patched_update_data):
        sheet = scraper.Spreadsheet(None, None)
        sheet.update_last_collection('rsync://localhost/ndt')
        patched_update_data.assert_called_once_with('rsync://localhost/ndt',
                                                    'lastcollectionattempt',
                                                    'x2016-01-28-07:43')

    @mock.patch.object(scraper.Spreadsheet, 'update_data')
    def test_update_mtime(self, patched_update_data):
        sheet = scraper.Spreadsheet(None, None)
        sheet.update_mtime('rsync://localhost/ndt', 7)
        patched_update_data.assert_called_once_with(
            'rsync://localhost/ndt', 'maxrawfilemtimearchived', 7)

    @mock.patch.object(scraper.Spreadsheet, 'update_data')
    def test_spreadsheet_log_handler(self, patched_update_data):
        sheet = scraper.Spreadsheet(None, None)
        loghandler = scraper.SpreadsheetLogHandler('rsync://local/ndt', sheet)
        logger = logging.getLogger('temp_test')
        logger.setLevel(logging.ERROR)
        logger.addHandler(loghandler)
        logger.info('INFORMATIVE')
        patched_update_data.assert_not_called()
        logger.error('BADNESS')
        patched_update_data.assert_called_once()

    def test_attempt_decompression(self):
        try:
            temp_d = tempfile.mkdtemp()
            with scraper.chdir(temp_d):
                file('test', 'w').write('testdata')
                subprocess.check_call(['/bin/gzip', 'test'])
                self.assertTrue(os.path.exists('test.gz'))
                self.assertFalse(os.path.exists('test'))
                self.assertEqual(
                    'test',
                    scraper.attempt_decompression('/bin/gunzip', 'test.gz'))
                self.assertFalse(os.path.exists('test.gz'))
                self.assertTrue(os.path.exists('test'))
        finally:
            shutil.rmtree(temp_d)

    def test_attempt_decompression_gunzip_failure(self):
        try:
            temp_d = tempfile.mkdtemp()
            with scraper.chdir(temp_d):
                file('test', 'w').write('testdata')
                subprocess.check_call(['/bin/gzip', 'test'])
                self.assertTrue(os.path.exists('test.gz'))
                self.assertFalse(os.path.exists('test'))
                self.assertEqual(
                    'test.gz',
                    scraper.attempt_decompression('/bin/false', 'test.gz'))
        finally:
            shutil.rmtree(temp_d)

    def test_attempt_decompression_disappearing_file(self):
        try:
            temp_d = tempfile.mkdtemp()
            with scraper.chdir(temp_d):
                file('test', 'w').write('testdata')
                subprocess.check_call(['/bin/gzip', 'test'])
                self.assertTrue(os.path.exists('test.gz'))
                self.assertFalse(os.path.exists('test'))
                self.assertEqual(
                    'test.gz',
                    scraper.attempt_decompression('/bin/true', 'test.gz'))
        finally:
            shutil.rmtree(temp_d)

    def test_attempt_decompression_no_clobber(self):
        try:
            temp_d = tempfile.mkdtemp()
            with scraper.chdir(temp_d):
                file('test', 'w').write('testdata')
                subprocess.check_call(['/bin/gzip', '--keep', 'test'])
                self.assertTrue(os.path.exists('test.gz'))
                self.assertTrue(os.path.exists('test'))
                self.assertEqual(
                    'test',
                    scraper.attempt_decompression('/bin/gunzip', 'test.gz'))
                self.assertTrue(os.path.exists('test.gz'))
                self.assertTrue(os.path.exists('test'))
        finally:
            shutil.rmtree(temp_d)

if __name__ == '__main__':  # pragma: no cover
    unittest.main()
