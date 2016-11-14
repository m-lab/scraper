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
"""Download all new data from an MLab node, then upload what can be uploaded.

This is a single-shot program to download data from an MLab node and then
upload it to Google Cloud Storage.  It is expected that this program will be
called every hour (or so) by a cron job, and that there will be many such cron
jobs running in a whole fleet of containers run by Google Container Engine.
"""

import argparse
import atexit
import contextlib
import datetime
import logging
import os
import subprocess
import sys
import tempfile

import fasteners


def acquire_lock_or_die(lockfile):
    """Prevents long-running downloads from being stepped on.

    Create the lockfile and write the pid to that file.  If the lockfile already
    exists, exit.

    Args:
      lockfile: the filename of the file to create
    """
    lock = fasteners.InterProcessLock(lockfile)
    if not lock.acquire(blocking=False):
        logging.critical('Lock on %s could not be acquired. Old job is likely '
                         'still running. Aborting.', lockfile)
        sys.exit(1)
    with file(lockfile, 'w') as lockfile:
        print >> lockfile, 'PID of scraper is', os.getpid()
    return lock


def assert_mlab_hostname(hostname):
    """Verifies that the passed-in hostname is a valid MLab hostname.

    This function is written in this way so that it can be used as part of
    command-line argument parsing.  The hostname should be something like
    mlab4.sea02.measurement-lab.org or perhaps
    ndt.iupui.mlab1.nuq0t.measurement-lab.org

    Returns:
      The valid hostname

    Raises:
      AssertionError if it is not valid
    """
    assert hostname.endswith('.measurement-lab.org')
    assert hostname.split('.') >= 4
    return hostname


def parse_cmdline(args):
    """Parse the commandline arguments.

    Args:
      args: the command-line arguments, minus the name of the binary

    Returns:
      the results of ArgumentParser.parse_args
    """
    parser = argparse.ArgumentParser(
        description='Scrape a single experiment at a site, upload the results '
        'if enough time has passed.')
    parser.add_argument(
        '--rsync_host',
        metavar='HOST',
        type=assert_mlab_hostname,
        required=True,
        help='The host to connect to over rsync')
    parser.add_argument(
        '--lockfile_dir',
        metavar='DIR',
        type=str,
        required=True,
        help='The the directory for lockfiles to prevent old jobs and new jobs '
        'from running simultaneously')
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
        '--spreadsheet',
        metavar='URL',
        type=str,
        required=True,
        help='The google doc ID of the spreadsheet used to sync download '
        'information with the nodes.')
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
    return parser.parse_args(args)


# Use IPv4, compression, and limit total bandwidth usage to 10 Mbps
RSYNC_ARGS = ['-4', '-z', '--bwlimit', '10000']


def list_rsync_files(rsync_binary, rsync_url):
    """Get a list of all files in the rsync module on the server.

    Lists all the files we might wish to download from the server. Be
    careful with the filenames this command returns, because they might
    not be safe for shell interpretation. Therefore, make sure that they
    are never interpreted by a shell. In order to ensure that, we need
    to have the full authoritative path to the rsync binary, which is the
    thing we pass in here.  If something goes wrong with the download, exit.

    Args:
      rsync_binary: the full path location of rsync
      rsync_url: the rsync:// url to download the list from

    Returns:
      a list of filenames
    """
    try:
        logging.info('rsync file list discovery from %s', rsync_url)
        command = [rsync_binary, '--list-only', '-r'] + \
            RSYNC_ARGS + [rsync_url]
        logging.info('Listing files on server with the command: %s',
                     ' '.join(command))
        lines = subprocess.check_output(command).splitlines()
        files = []
        for line in lines:
            # None is a special whitespace arg for split
            chunks = line.split(None, 4)
            if len(chunks) != 5:
                logging.error('Bad line in output: %s', line)
                continue
            files.append(chunks[4])
        return files
    except subprocess.CalledProcessError as error:
        logging.error('rsync file listing failed: %s', str(error))
        sys.exit(1)


def get_progress_from_spreadsheet(spreadsheet, rsync_url):  # pragma: no cover
    # TODO(pboothe)
    return datetime.datetime(2016, 10, 15).date()


def remove_older_files(date, files):
    """Creates a new list with all files at least as old as `date` removed.

    Args:
      date: the date of the last day to remove from consideration
      files: the list of filenames

    Returns:
      a filtered list of filenames
    """
    filtered = []
    for fname in files:
        if fname.count('/') < 3:
            logging.info('Ignoring %s on the assumption it is a directory',
                         fname)
            continue
        year, month, day, _ = fname.split('/', 3)
        if not (year.isdigit() and month.isdigit() and day.isdigit()):
            logging.error(
                'Bad filename. Was supposed to be YYYY/MM/DD, but was %s',
                fname)
            continue
        # Pass in a radix to guard against zero-padded 8 and 9
        if datetime.date(int(year, 10), int(month, 10), int(day, 10)) > date:
            filtered.append(fname)
    return filtered


def download_files(rsync_binary, rsync_url, files, destination):
    """Downloads the files from the server.

    The filenames may not be safe for shell interpretation, so make sure
    they are never interpreted by a shell.  If something goes wrong with
    the download, exit.

    Args:
      rsync_binary: The full path to `rsync`
      rsync_url: The url from which to retrieve the files
      files: a list of filenames to retrieve
      destination: the directory on the local host to put the files
    """
    if not files:
        logging.warning('No files to be downloaded from %s', rsync_url)
        return
    # Rsync all the files that are new enough for us to care about
    with tempfile.NamedTemporaryFile() as temp:
        # Write the list of files to a tempfile, so as not to have to worry
        # about too-long command lines full of filenames.
        for fname in files:
            print >> temp, fname
        temp.flush()
        # Download all the files.
        try:
            logging.info('Downloading %d files', len(files))
            command = [rsync_binary, '--files-from', temp.name
                       ] + RSYNC_ARGS + [rsync_url, destination]
            subprocess.check_call(command)
        except subprocess.CalledProcessError as error:
            logging.error('rsync download failed: %s', str(error))
            sys.exit(1)
        logging.info('sync completed successfully from %s', rsync_url)


def max_new_high_water_mark():
    """The most recent date that we could consider "old enough" to upload.

    8 hours after midnight, we will assume that no tests from the previous
    day could possibly have failed to be written to disk.  So this should
    always be either yesterday, or the day before, depending on how late
    in the day it is.

    Returns:
      The most recent day whose data is safe to upload.
    """
    return (datetime.datetime.utcnow() - datetime.timedelta(
        days=1, hours=8)).date()


def find_all_days_to_upload(localdir, high_water_mark):
    """Find all the days that are eligible to be uploaded.

    Search through localdir, trying to find all the data that is from a day that
    is old enough to be uploaded.

    Args:
      localdir: the local directory containing all the data
      high_water_mark: the most recent day that is eligible to be uploaded

    Yields:
      a sequence of days that exist on the localhost and are old enough to be
      uploaded.
    """
    for year in sorted(filter(str.isdigit, os.listdir(localdir))):
        year_dir = os.path.join(localdir, year)
        if not os.path.isdir(year_dir):
            continue
        for month in sorted(filter(str.isdigit, os.listdir(year_dir))):
            month_dir = os.path.join(localdir, year, month)
            if not os.path.isdir(month_dir):
                continue
            for day in sorted(filter(str.isdigit, os.listdir(month_dir))):
                date_dir = os.path.join(localdir, year, month, day)
                if not os.path.isdir(date_dir):
                    continue
                # Make sure to specify radix 10 to prevent an octal
                # interpretation of 0-padded single digits 08 and 09.
                try:
                    date = datetime.date(
                        year=int(year, 10),
                        month=int(month, 10),
                        day=int(day, 10))
                    if date <= high_water_mark:
                        yield date
                except ValueError as verr:
                    logging.error('Bad directory that looks like a day: %s %s',
                                  date_dir, verr)


@contextlib.contextmanager
def chdir(directory):
    """Change the working directory for the duration of a `with` statement.

    From http://benno.id.au/blog/2013/01/20/withfail which fills a sort of
    obvious niche that one would hope would exist as part of the os library.

    Args:
      directory: the directory to change to
    """
    cwd = os.getcwd()
    os.chdir(directory)
    try:
        yield
    finally:
        os.chdir(cwd)


def create_tarfile(tar_binary, tarfile_name, component_files):
    """Creates a tarfile in the current directory.

    Args:
      tar_binary: the full path to the tar binary
      tarfile_name: the name of the tarfile to create, including extension
      component_files: a list of filenames to put in that tarfile

    Raises:
      SystemExit if anything fails
    """
    if os.path.exists(tarfile_name):
        logging.error('The file %s/%s already exists, which is preventing the '
                      'creation of another file of the same name',
                      os.getcwd(), tarfile_name)
        sys.exit(1)
    command = [tar_binary, 'cfz', tarfile_name] + component_files
    try:
        subprocess.check_call(command)
    except subprocess.CalledProcessError as error:
        logging.error('tarfile creation ("%s") failed: %s', ' '.join(command),
                      str(error))
        sys.exit(1)
    if not os.path.exists(tarfile_name):
        logging.error('The tarfile %s/%s was not successfully created',
                      os.getcwd(), tarfile_name)
        sys.exit(1)


def node_and_site(host):
    """Determine the host and site from the hostname.

    Returns the host and site an contained in the hostname of the mlab
    node.  Strips .measurement-lab.org from the hostname if it exists.
    Existing files have names like 20150706T000000Z-
    mlab1-acc01-ndt-0000.tgz and this function is designed to return the
    pair ('mlab1', 'acc01') as derived from a hostname like
    'ndt.iupui.mlab2.nuq1t.measurement-lab.org'
    """
    assert_mlab_hostname(host)
    names = host.split('.')
    return (names[-4], names[-3])


def create_tarfiles(tar_binary, directory, day, host, experiment,
                    max_uncompressed_size):
    """Create tarfiles, and yield the name of each tarfile as it is made.

    Because one day may contain a lot of data, we create a series of tarfiles,
    none of which may contain more than max_uncompressed_size buytes of data.

    Args:
      tar_binary: the full pathname for the tar binary
      directory: the directory at the root of the file hierarchy
      day: the date for the tarfile
      host: the hostname for the tar file
      experiment: the experiment with data contained in this tarfile
      max_uncompressed_size: the max size of an individual tarfile
    """
    node, site = node_and_site(host)
    # Existing files have names like: 20150706T000000Z-mlab1-acc01-ndt-0000.tgz
    filename_prefix = '%d%02d%02dT000000Z-%s-%s-%s-' % (
        day.year, day.month, day.day, node, site, experiment)
    filename_suffix = '.tgz'
    day_dir = '%d/%02d/%02d' % (day.year, day.month, day.day)
    tarfile_size = 0
    tarfile_files = []
    tarfile_index = 0
    with chdir(directory):
        for filename in sorted(os.listdir(day_dir)):
            filename = os.path.join(day_dir, filename)
            filesize = os.stat(filename).st_size
            if (tarfile_files and
                    tarfile_size + filesize > max_uncompressed_size):
                tarfile_name = '%s%04d%s' % (filename_prefix, tarfile_index,
                                             filename_suffix)
                create_tarfile(tar_binary, tarfile_name, tarfile_files)
                logging.info('Created %s', tarfile_name)
                yield tarfile_name
                tarfile_files = []
                tarfile_size = 0
                tarfile_index += 1
            tarfile_files.append(filename)
            tarfile_size += filesize
        if tarfile_files:
            tarfile_name = '%s%04d%s' % (filename_prefix, tarfile_index,
                                         filename_suffix)
            create_tarfile(tar_binary, tarfile_name, tarfile_files)
            logging.info('Created %s', tarfile_name)
            yield tarfile_name


def upload_tarfile(tgz_filename):  # pragma: no cover
    # TODO(pboothe)
    pass


def update_high_water_mark(spreadsheet, rsync_url, day):  # pragma: no cover
    # TODO(pboothe)
    pass


def remove_datafiles(directory, day):  # pragma: no cover
    # TODO(pboothe)
    pass


def main():  # pragma: no cover
    # TODO(pboothe) end-to-end tests
    args = parse_cmdline(sys.argv[1:])

    # Ensure that old long-lasting downloads don't get clobbered by new ones.
    lockfile = os.path.join(
        args.lockfile_dir,
        '{}_{}.lock'.format(args.rsync_host, args.rsync_module))
    lock = acquire_lock_or_die(lockfile)
    atexit.register(lock.release)

    # If the destination directory does not exist, make it exist.
    destination = os.path.join(args.data_dir, args.rsync_host,
                               args.rsync_module)
    if not os.path.isdir(destination):
        os.makedirs(destination)

    # Get the file list and then the files from the server.
    rsync_url = 'rsync://{}:{}/{}/'.format(args.rsync_host, args.rsync_port,
                                           args.rsync_module)
    files = list_rsync_files(args.rsync_binary, rsync_url)
    progress_level = get_progress_from_spreadsheet(args.spreadsheet, rsync_url)
    files = remove_older_files(progress_level, files)
    download_files(args.rsync_binary, rsync_url, files, destination)

    # Tar up what we have for each un-uploaded day that is sufficiently in the
    # past (up to and including the new high water mark), upload what we have,
    # and delete the local copies of all successfully-uploaded data.
    new_high_water_mark = max_new_high_water_mark()
    for day in find_all_days_to_upload(destination, new_high_water_mark):
        for tgz_filename in create_tarfiles(args.tar_binary, destination, day,
                                            args.rsync_host, args.rsync_module,
                                            args.max_uncompressed_size):
            upload_tarfile(tgz_filename)
            os.remove(tgz_filename)
        update_high_water_mark(args.spreadsheet, rsync_url, day)
        remove_datafiles(destination, day)


if __name__ == '__main__':  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s %(levelname)s %(filename)s:%(lineno)d] '
        '%(message)s')
    main()
