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

import datetime
import os
import shutil
import subprocess
import tempfile
import unittest

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

    @mock.patch('fasteners.InterProcessLock.acquire')
    def test_file_locking_failure_causes_exit(self, patched_acquire):
        try:
            temp_d = tempfile.mkdtemp()
            lockfile = os.path.join(temp_d, 'testlockfile')
            patched_acquire.return_value = False
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

    @mock.patch('subprocess.check_output')
    def test_list_rsync_files(self, patched_subprocess):
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
        patched_subprocess.return_value = serverfiles
        files = scraper.list_rsync_files('/usr/bin/rsync', 'localhost')
        self.assertEqual([
            '.', '2016', '2016/01', '2016/01/06', '2016/01/06/.gz',
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
        files = [
            '.', '2016', '2016/01', '2016/01/06', '2016/01/06/.gz',
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
            '2016/10/26/20161026T18:02:59.898385000Z_eb.measurementlab.net:52410.c2s_snaplog.gz'
        ]
        filtered = scraper.remove_older_files(
            datetime.datetime(2016, 10, 25).date(), files)
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

    def test_download_files_fails_and_dies(self):
        with self.assertRaises(SystemExit):
            scraper.download_files('/bin/false', 'localhost/',
                                   ['2016/10/26/DNE1', '2016/10/26/DNE2'],
                                   '/tmp')

    def test_download_files_with_empty_does_nothing(self):
        # If the next line doesn't raise SystemExit then the test passes
        scraper.download_files('/bin/false', 'localhost/', [], '/tmp')

    @mock.patch('subprocess.check_call')
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
                os.makedirs('2016/07/05')
                os.makedirs('2016/07/monkey')
                os.makedirs('2016/monkey/monkey')
                os.makedirs('monkey/monkey/monkey')
                os.makedirs('2016/07/06')
                os.makedirs('2015/10/9001')
                os.makedirs('2016/07/07')
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
                    raise Exception()
            except Exception:
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
                file('2016/01/28/test1.txt', 'w').write('hello')
                file('2016/01/28/test2.txt', 'w').write('goodbye')
            files = list(
                scraper.create_tarfiles('/bin/tar', temp_d,
                                        datetime.date(2016, 1, 28),
                                        'mlab9.dne04.measurement-lab.org',
                                        'exper', 100000))
            self.assertEqual(files,
                             ['20160128T000000Z-mlab9-dne04-exper-0000.tgz'])
            with scraper.chdir(temp_d):
                for fname in files:
                    self.assertTrue(os.path.isfile(fname))
                shutil.rmtree('2016')
                self.assertFalse(os.path.exists('2016/01/28/test1.txt'))
                self.assertFalse(os.path.exists('2016/01/28/test2.txt'))
                subprocess.check_call([
                    '/bin/tar', 'xfz',
                    '20160128T000000Z-mlab9-dne04-exper-0000.tgz'
                ])
                self.assertTrue(os.path.exists('2016/01/28/test1.txt'))
                self.assertTrue(os.path.exists('2016/01/28/test2.txt'))
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
            files = list(
                scraper.create_tarfiles('/bin/tar', temp_d,
                                        datetime.date(2016, 1, 28),
                                        'mlab9.dne04.measurement-lab.org',
                                        'exper', 4))
            self.assertEqual(files, [
                '20160128T000000Z-mlab9-dne04-exper-0000.tgz',
                '20160128T000000Z-mlab9-dne04-exper-0001.tgz'
            ])

            with scraper.chdir(temp_d):
                for fname in files:
                    self.assertTrue(os.path.isfile(fname))
                shutil.rmtree('2016')
                self.assertFalse(os.path.exists('2016/01/28/test1.txt'))
                self.assertFalse(os.path.exists('2016/01/28/test2.txt'))
                subprocess.check_call([
                    '/bin/tar', 'xfz',
                    '20160128T000000Z-mlab9-dne04-exper-0000.tgz'
                ])
                self.assertTrue(os.path.exists('2016/01/28/test1.txt'))
                self.assertFalse(os.path.exists('2016/01/28/test2.txt'))
                shutil.rmtree('2016')
                subprocess.check_call([
                    '/bin/tar', 'xfz',
                    '20160128T000000Z-mlab9-dne04-exper-0001.tgz'
                ])
                self.assertFalse(os.path.exists('2016/01/28/test1.txt'))
                self.assertTrue(os.path.exists('2016/01/28/test2.txt'))
        finally:
            shutil.rmtree(temp_d)


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
