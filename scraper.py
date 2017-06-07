#!/usr/bin/python -u
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

"""Download all new data from an MLab node, then upload what can be uploaded.

This is a library to download data from an MLab node, upload data to
Google Cloud Storage, and update Cloud Datastore with the status.  Because of
the vagaries of the discovery API and some command-line options that need to be
set, the init() function in this library should be the first thing called.

This program expects to be run on GCE and uses cloud APIs with the default
credentials available to a GCE instance.
"""

import contextlib
import datetime
import logging
import os
import re
import shutil
import subprocess
import tempfile

import apiclient
import googleapiclient.errors
import prometheus_client
import retry

from oauth2client.contrib import gce
import google.cloud.datastore as cloud_datastore


# Three kinds of exception, a base and two that contain hints about what can be
# done next.
class ScraperException(Exception):
    """Base class for exceptions in scraper."""

    def __init__(self, prometheus_label, message):
        super(ScraperException, self).__init__(message)
        self.prometheus_label = prometheus_label


class RecoverableScraperException(ScraperException):
    """Exceptions where it is better to retry than crash."""


class NonRecoverableScraperException(ScraperException):
    """Exceptions where it is better to crash than retry."""


# Prometheus histogram buckets are web-response-sized by default, with lots of
# sub-second buckets and very few multi-second buckets.  We need to change them
# to rsync-download-sized, with lots of multi-second buckets up to even a
# multi-hour bucket or two.  The precise choice of bucket values below is a
# compromise between exponentially-sized bucket growth and a desire to make
# sure that the bucket sizes are nice round time units.
TIME_BUCKETS = (1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
                1800.0, 3600.0, 7200.0, float('inf'))

# These are the quantities monitored by prometheus
BYTES_UPLOADED = prometheus_client.Counter(
    'scraper_bytes_uploaded',
    'Total bytes uploaded to GCS',
    ['bucket'])
FILES_UPLOADED = prometheus_client.Counter(
    'scraper_files_uploaded',
    'Total file count of the files uploaded to GCS',
    ['bucket'])
# The prometheus_client libraries confuse the linter.
# pylint: disable=no-value-for-parameter
RSYNC_LIST_FILES_RUNS = prometheus_client.Histogram(
    'scraper_rsync_list_runtime_seconds',
    'How long each rsync list-files op took',
    buckets=TIME_BUCKETS)
RSYNC_FILE_CHUNK_DOWNLOADS = prometheus_client.Histogram(
    'scraper_rsync_chunk_download_runtime_seconds',
    'How long each rsync download of a 1000-file chunk took',
    buckets=TIME_BUCKETS)
TARFILE_CREATION_TIME = prometheus_client.Histogram(
    'scraper_per_tarfile_creation_runtime_seconds',
    'How long it took to make each tarfile',
    buckets=TIME_BUCKETS)
TARFILE_UPLOAD_TIME = prometheus_client.Histogram(
    'scraper_per_tarfile_upload_time_seconds',
    'How long it took to upload each tarfile',
    buckets=TIME_BUCKETS)
TARFILE_CHUNK_UPLOAD_TIME = prometheus_client.Histogram(
    'scraper_tarfile_chunk_upload_time_seconds',
    'How long it took to upload each tarfile chunk')
# pylint: enable=no-value-for-parameter


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
    assert re.match(
        r'^(.*\.)?mlab[1-9]\.[a-z]{3}[0-9][0-9t]\.measurement-lab\.org$',
        hostname), 'Bad hostname: "%s"' % hostname
    return hostname


# Use IPv4, compression, limit total bandwidth usage to 10 Mbps, don't wait
# too long before bailing out, and make sure to chmod the files to have sensible
# permissions.
RSYNC_ARGS = ['-4', '-avvz', '--bwlimit=10000', '--timeout=300',
              '--contimeout=300', '--chmod=u=rwX']


@RSYNC_LIST_FILES_RUNS.time()
def list_rsync_files(rsync_binary, rsync_url, destination):
    """Get a list of all files in the rsync module on the server.

    Lists all the files we might wish to download from the server. Be
    careful with the filenames this command returns, because they might
    not be safe for shell interpretation. Therefore, make sure that they
    are never interpreted by a shell.

    Args:
      rsync_binary: the full path location of rsync
      rsync_url: the rsync:// url to download the list from

    Returns:
      a list of filenames

    Raises:
      RecoverableScraperException when rsync doesn't run successfully
    """
    logging.info('rsync file list discovery from %s', rsync_url)
    # A command that works to upgrade things incrementally is:
    #  /usr/bin/rsync -4 -avvzn --bwlimit 10000 --timeout 120 --contimeout 120 \
    #     rsync://ndt.iupui.mlab2.lba01.measurement-lab.org:7999/ndt \
    #     scraper_data/ndt.iupui.mlab2.lba01.measurement-lab.org/ndt
    # Most other codepaths on the rsync server seem to wait until the filelist
    # is complete before sending the list of files, and doing that can, in
    # extreme cases, mean that the socket times out and leaves the local scraper
    # rsync in a half-open state.
    #
    # Here is an example output from that command:
    #  opening tcp connection to ndt.iupui.mlab2.lba01.measurement-lab.org port
    #  7999
    #  sending daemon args: --server --sender -vvnlogDtprze.iLsfx --timeout=300
    #  --bwlimit=10000 . ndt/  (7 args)
    #  receiving incremental file list
    #  delta-transmission enabled
    #  [receiver] expand file_list pointer array to 524288 bytes, did move
    #  [generator] expand file_list pointer array to 524288 bytes, did move
    #  [receiver] expand file_list pointer array to 1048576 bytes, did move
    #  [generator] expand file_list pointer array to 1048576 bytes, did move
    #  [receiver] expand file_list pointer array to 2097152 bytes, did move
    #  [generator] expand file_list pointer array to 2097152 bytes, did move
    #  ./
    #  2017/05/
    #  2017/06/
    #  2017/06/01/
    #  2017/06/02/
    #  2017/06/03/
    #  2017/06/03/.gz
    #  2017/06/03/20170603T23:59:47.143624000Z_86.164.175.237.s2c_ndttrace.gz
    #  2017/06/03/20170603T23:59:54.535055000Z_95.151.122.146:50901.meta
    # [snip]
    # The lines with [generator] and [receiver] may happen at any point in the
    # output.
    command = ([rsync_binary, '-n'] + RSYNC_ARGS + [rsync_url, destination])
    logging.info('Listing files on server with the command: %s',
                 ' '.join(command))
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    files = []
    # Only download things that are files and that respect the date-based
    # directory structure.
    files_regex = re.compile(r'^\d{4}/\d\d/\d\d/.*[^/]$')
    for line in process.stdout:
        line = line.strip()
        if files_regex.match(line):
            files.append(line)
        if len(files) % 1000 == 0:
            logging.info('Found %d files to download so far', len(files))
    logging.info('Found %d files to download', len(files))
    process.wait()
    logging.info('rsync process exited with code %d', process.returncode)
    # Return code 24 from rsync is "partial transfer because some files
    # disappeared", which is totally fine with us - ephemeral files disappearing
    # is no cause for alarm.
    if process.returncode not in (0, 24):
        message = 'rsync file listing failed (%d): %s' % (process.returncode,
                                                          process.stderr.read())
        logging.error(message)
        raise RecoverableScraperException('rsync_listing', message)
    return files


def remove_older_files(date, files):
    """Yields all well-formed filenames newer than `date`.

    Args:
      date: the date of the last day to remove from consideration
      files: the list of filenames

    Yields:
      a sequence of filenames
    """
    for fname in files:
        if fname.count('/') < 3:
            logging.info('Ignoring %s (if it is a directory, the directory '
                         'contents will still be examined)', fname)
            continue
        year, month, day, _ = fname.split('/', 3)
        if not (year.isdigit() and month.isdigit() and day.isdigit()):
            logging.error(
                'Bad filename. Was supposed to be YYYY/MM/DD, but was %s',
                fname)
            continue
        try:
            # Pass in a radix to guard against zero-padded 8 and 9.
            year = int(year, 10)
            month = int(month, 10)
            day = int(day, 10)
            if datetime.date(year, month, day) > date:
                yield fname
        except ValueError as verr:
            logging.warning('Bad filename (%s) caused bad date: %s', fname,
                            verr)


# Download files 1000 at a time to help keep rsync memory usage low.
#    https://rsync.samba.org/FAQ.html#5
FILES_PER_RSYNC_DOWNLOAD = 1000


def download_files(rsync_binary, rsync_url, files, destination):
    """Downloads the files from the server.

    The filenames may not be safe for shell interpretation, so make sure
    they are never interpreted by a shell.  If something goes wrong with
    the download, exit.

    Args:
      rsync_binary: The full path to `rsync`
      rsync_url: The url from which to retrieve the files
      files: an iterable of filenames to retrieve
      destination: the directory on the local host to put the files
    """
    files = list(files)
    if len(files) == 0:
        logging.info('No files to be downloaded from %s', rsync_url)
        return
    # Rsync all the files passed in
    for start in range(0, len(files), FILES_PER_RSYNC_DOWNLOAD):
        with RSYNC_FILE_CHUNK_DOWNLOADS.time():
            filenames = files[start:start + FILES_PER_RSYNC_DOWNLOAD]
            with tempfile.NamedTemporaryFile() as temp:
                # Write the list of files to a tempfile, so as not to have to
                # worry about too-long command lines full of filenames.
                temp.write('\0'.join(filenames))
                temp.flush()
                # Download all the files.
                logging.info('Synching %d files (already synched %d/%d)',
                             len(filenames), start, len(files))
                # Don't crash when ephemeral files disappear.
                # Filenames in the temp file are null-separated.
                # The filenames to transfer are in a file.
                command = ([rsync_binary] + RSYNC_ARGS +
                           ['--from0', '--files-from', temp.name, rsync_url,
                            destination])
                error_code = subprocess.call(command)
                if error_code not in (0, 24):
                    message = 'rsync download failed exit code: %d' % error_code
                    logging.error(message)
                    raise RecoverableScraperException('rsync_download', message)
    logging.info('sync completed successfully from %s', rsync_url)


def max_new_archived_date():
    """The most recent date that we could consider "old enough" to upload.

    8 hours after midnight, we will assume that no tests from the previous
    day could possibly have failed to be written to disk.  So this should
    always be either yesterday or the day before, depending on how late
    in the day it is.

    Returns:
      The most recent day whose data is safe to upload.
    """
    return (datetime.datetime.utcnow() - datetime.timedelta(
        days=1, hours=8)).date()


def datetime_to_epoch(datetime_value):
    """Converts a datetime value into seconds since epoch.

    This should be a member function of the datetime class, but they did not see
    fit to provide this functionality.
    """
    epoch = datetime.datetime(year=1970, month=1, day=1)
    return int((datetime_value - epoch).total_seconds())


def find_all_days_to_upload(localdir, candidate_last_archived_date):
    """Find all the days that are eligible to be uploaded.

    Search through localdir, trying to find all the data that is from a day that
    is old enough to be uploaded.

    Args:
      localdir: the local directory containing all the data
      candidate_last_archived_date: the most recent day that is eligible to be
                                    uploaded

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
                    if date <= candidate_last_archived_date:
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


@TARFILE_CREATION_TIME.time()
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
        logging.warning('The file %s/%s already exists, which will prevent the '
                        'creation of another file of the same name. We are '
                        'deleting it.',
                        os.getcwd(), tarfile_name)
        os.remove(tarfile_name)

    command = [tar_binary, 'cfz', tarfile_name, '--null', '--files-from']
    try:
        with tempfile.NamedTemporaryFile() as temp:
            temp.write('\0'.join(component_files))
            temp.flush()
            command.append(temp.name)
            subprocess.check_call(command)
    except subprocess.CalledProcessError as error:
        message = 'tarfile creation ("%s") failed: %s' % (' '.join(command),
                                                          str(error))
        logging.error(message)
        raise NonRecoverableScraperException('tar_error', message)
    if not os.path.exists(tarfile_name):
        message = ('The tarfile %s/%s was not successfully created' %
                   (os.getcwd(), tarfile_name))
        logging.error(message)
        raise NonRecoverableScraperException('no_tar_file', message)


def node_and_site(host):
    """Determine the host and site from the hostname.

    Returns the host and site contained in the hostname of the mlab node. Strips
    .measurement-lab.org from the hostname if it exists. Existing files have
    names like 20150706T000000Z-mlab1-acc01-ndt-0000.tgz and this function is
    designed to return the pair ('mlab1', 'acc01') as derived from a hostname
    like 'ndt.iupui.mlab2.nuq1t.measurement-lab.org'
    """
    assert_mlab_hostname(host)
    names = host.split('.')
    return (names[-4], names[-3])


def all_files(directory):
    """Lists all files in all subdirectories beneath the current dir."""
    for root, _dirs, files in os.walk(directory):
        for filename in files:
            yield os.path.join(root, filename)


def timestamp_from_filename(filename):
    """Turns a filename into a timestamp or None.

    Filenames are strings like:
      2017/05/10/20170510T15:05:01.520348000Z_12.30.237.162.c2s_ndttrace

    and the associated timestamp for that filename is:
      datetime.datetime(2017, 5, 10, 15, 5, 1.520348000)
    """
    basename = os.path.basename(filename)
    timestamp_regex = re.compile(
        r'^(\d{4})(\d\d)(\d\d)T(\d\d):(\d\d):(\d\d)\.(\d{6})')
    #         1      2    3      4      5      6        7
    #       year   month day   hour  minutes seconds microseconds
    match = timestamp_regex.match(basename)
    if match is None:
        return None
    else:
        return datetime.datetime(int(match.group(1), 10),
                                 int(match.group(2), 10),
                                 int(match.group(3), 10),
                                 int(match.group(4), 10),
                                 int(match.group(5), 10),
                                 int(match.group(6), 10),
                                 int(match.group(7), 10))


def create_tarfilename_template(day, host, experiment, tarfile_directory):
    """Create a template tarfile name from the passed-in values.

    Tarfile names look like:
      '20150706T000000Z-mlab1-acc01-ndt-0000.tgz'

    Where the final 0000 is a counter for the tarfiles for that site for that
    day, which means that for that file this function would return the template:
      '20150706T000000Z-mlab1-acc01-ndt-%04d.tgz'
    """
    node, site = node_and_site(host)
    filename_prefix = '%d%02d%02dT000000Z-%s-%s-%s-' % (
        day.year, day.month, day.day, node, site, experiment)
    filename_suffix = '.tgz'
    return os.path.join(tarfile_directory,
                        filename_prefix + '%04d' + filename_suffix)


def create_temporary_tarfiles(tar_binary, tarfile_template, directory, day,
                              max_uncompressed_size):
    """Create tarfiles, and yield the name of each tarfile as it is made.

    Because one day may contain a lot of data, we create a series of tarfiles,
    none of which may contain more than max_uncompressed_size buytes of data.
    Upon resumption, remove the tarfile that was created.

    Args:
      tar_binary: the full pathname for the tar binary
      tarfile_template: a string to serve as the tarfile filename template
      directory: the directory at the root of the file hierarchy
      day: the date for the tarfile
      max_uncompressed_size: the max size of an individual tarfile

    Yields:
      A tuple of the name of the tarfile created, the most recent mtime of
      any tarfile's component files, and the number of files in the tarfile
    """
    day_dir = '%d/%02d/%02d' % (day.year, day.month, day.day)
    tarfile_size = 0
    tarfile_files = []
    tarfile_index = 0
    max_mtime = 0
    prev_timestamp = None
    with chdir(directory):
        for filename in sorted(all_files(day_dir)):
            file_timestamp = timestamp_from_filename(filename)
            filestat = os.stat(filename)
            filesize = filestat.st_size
            max_mtime = max(max_mtime, int(filestat.st_mtime))
            if (tarfile_files and
                    tarfile_size + filesize > max_uncompressed_size and
                    (file_timestamp is None or
                     file_timestamp != prev_timestamp)):
                tarfile_name = tarfile_template % tarfile_index
                create_tarfile(tar_binary, tarfile_name, tarfile_files)
                logging.info('Created local file %s', tarfile_name)
                yield tarfile_name, max_mtime, len(tarfile_files)
                os.remove(tarfile_name)
                logging.info('Removed local file %s', tarfile_name)
                tarfile_files = []
                tarfile_size = 0
                tarfile_index += 1
            tarfile_files.append(filename)
            tarfile_size += filesize
            prev_timestamp = file_timestamp
        if tarfile_files:
            tarfile_name = tarfile_template % tarfile_index
            create_tarfile(tar_binary, tarfile_name, tarfile_files)
            logging.info('Created local file %s', tarfile_name)
            yield tarfile_name, max_mtime, len(tarfile_files)
            os.remove(tarfile_name)
            logging.info('Removed local file %s', tarfile_name)


# The GCS upload mechanism loads the item to be uploaded into RAM. This means
# that a 500 MB tarfile used that much RAM upon upload, and this caused our
# containers to OOM on busy servers.  The chunksize below specifies how much
# data to load into RAM, to help prevent OOM problems.
TARFILE_UPLOAD_CHUNK_SIZE = 10 * 1024 * 1024


@TARFILE_UPLOAD_TIME.time()
@retry.retry(exceptions=RecoverableScraperException,
             backoff=2,      # Exponential backoff with a multiplier of 2
             jitter=(1, 5),  # plus a random number of seconds from 1 to 5
             max_delay=300,  # but never more than 5 minutes.
             logger=logging.getLogger())
def upload_tarfile(service, tgz_filename, date, experiment,
                   bucket):  # pragma: no cover
    """Uploads a tarfile to Google Cloud Storage for later processing.

    Puts the file into a GCS bucket. If a file of that same name already exists,
    the file is overwritten.  If the upload fails, the upload is retried until
    it succeeds, although we perform exponential backoff with a maximum wait
    time of 5 minutes between attempts.  If the GCS service becomes unavailable
    in the longer term, then scraper won't work anyway, and retrying will work
    around temporary blips in service or network reachability.

    Args:
      service: the service object returned from discovery
      tgz_filename: the basename of the tarfile
      date: the date for the data
      experiment: the subdirectory of the bucket for this data
      bucket: the name of the GCS bucket
    """
    name = '%s/%d/%02d/%02d/%s' % (experiment, date.year, date.month, date.day,
                                   os.path.basename(tgz_filename))
    media = apiclient.http.MediaFileUpload(tgz_filename,
                                           chunksize=TARFILE_UPLOAD_CHUNK_SIZE,
                                           resumable=True)
    try:
        logging.info('Uploading %s to %s/%s', tgz_filename, bucket, name)
        request = service.objects().insert(
            bucket=bucket, name=name, media_body=media)
        response = None
        while response is None:
            with TARFILE_CHUNK_UPLOAD_TIME.time():
                progress, response = request.next_chunk()
                if progress:
                    logging.debug('Uploaded %d%%', 100.0 * progress.progress())
        logging.info('Upload to %s/%s complete!', bucket, name)
    except googleapiclient.errors.HttpError as error:
        if (error.resp.status // 100) == 5:  # HTTP 500 is recoverable
            logging.warning('Recoverable error on upload: ' + str(error))
            raise RecoverableScraperException('upload', str(error))
        else:
            logging.warning('Non-recoverable error on upload: ' + str(error))
            raise NonRecoverableScraperException('upload', str(error))


def remove_datafiles(directory, day):
    """Removes datafiles for a given day from the local disk.

    Prunes any empty subdirectories that it creates.
    """
    day_dir = '%02d' % day.day
    month_dir = '%02d' % day.month
    year_dir = '%d' % day.year
    with chdir(directory):
        with chdir(year_dir):
            with chdir(month_dir):
                shutil.rmtree(day_dir)
            if not os.listdir(month_dir):
                os.rmdir(month_dir)
        if not os.listdir(year_dir):
            os.rmdir(year_dir)


def xdate_to_date_or_die(xdate_text):
    """Converts a string of the form 'x2016-01-28' into a date."""
    try:
        assert xdate_text.count('-') == 2 and xdate_text[0] == 'x'
        year, month, day = xdate_text[1:].split('-')
        assert year.isdigit() and month.isdigit() and day.isdigit()
        return datetime.date(int(year, 10), int(month, 10), int(day, 10))
    except (AssertionError, ValueError):
        message = 'Bad date string: "%s"' % xdate_text
        logging.error(message)
        raise NonRecoverableScraperException('bad_date', message)


class SyncStatus(object):
    """Saves and retrieves the status of an rsync endpoint from Datastore.

    All get_* and update_* methods cause remote reads and writes to the Cloud
    Datastore instance associated with the current Google Cloud project.

    By design, every running scraper instance should be associated with one
    (and only one) datastore Entity, and every Entity should be associated with
    at most one running scraper instance.  To enforce this invariant, we use
    the namespace feature of cloud datastore in combination with the rsync url
    and the cloud project name (which is implicitly specified as the name of
    the project in which the instance is running).

    A separate process will repeatedly query cloud datastore to find every
    Entity in the project's Datastore within a particular namespace, and then
    update the coordinating spreadsheet to contain the same data as exists in
    cloud datastore.  The 'ground truth' of the system is maintained in cloud
    datastore, and the spreadsheet should be regarded as merely a display layer
    on top of that dataset.

    Failure to update the rsync cloud datastore cell would mean that data would
    build up on the nodes and the data deletion service would not know it could
    delete uploaded data.  If a rogue process updated the cloud datastore cell,
    then the node might delete data that had not yet been uploaded to the right
    cloud datastore bucket.
    """

    RSYNC_KEY = 'dropboxrsyncaddress'
    COLLECTION_KEY = 'lastsuccessfulcollection'
    DEBUG_MESSAGE_KEY = 'errorsincelastsuccessful'
    LAST_COLLECTION_KEY = 'lastcollectionattempt'
    MTIME_KEY = 'maxrawfilemtimearchived'

    def __init__(self, client, rsync_url):
        self._client = client
        self._rsync_url = rsync_url
        self._key = None
        self._entity = None

    # Retry required until
    # https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2694
    # is fixed.
    @retry.retry(tries=5)
    def get_data(self):
        """Retrieves data from cloud datastore.

        A separate function so that it can be mocked for testing purposes.
        """
        if self._key is None:
            self._key = self._client.key(SyncStatus.RSYNC_KEY, self._rsync_url)
        return self._client.get(self._key)

    def get_last_archived_date(self, default_date=datetime.date(2009, 1, 1)):
        """Returns the most recent date from which we have all the data.

        Used to determine what local data on a node has been archived and is
        safe to delete, and also what data must be downloaded from a node, and
        what data need not be downloaded.  Other than exceptional recovery
        cases, this quantity must must be monotonically increasing.

        Args:
          default_date: the date to return if no datastore entry exists
        """
        value = self.get_data()
        if not value:
            logging.info('No data found in the datastore')
            return default_date
        elif (SyncStatus.COLLECTION_KEY not in value or
              not value[SyncStatus.COLLECTION_KEY]):
            logging.info('Data in the datastore had no %s',
                         SyncStatus.COLLECTION_KEY)
            return default_date
        else:
            return xdate_to_date_or_die(value[SyncStatus.COLLECTION_KEY])

    # Retry required until
    # https://github.com/GoogleCloudPlatform/google-cloud-python/issues/2694
    # is fixed.
    @retry.retry(tries=5)
    def update_data(self, entry_key, entry_value):
        """Updates a datastore value.

        If no value for the key exists, then one will be created.

        Args:
          entry_key: must be one of the static values in SyncStatus
          entry_value: the new value to write to the datastore entry
        """
        value = self.get_data()
        if not value:
            logging.info('Key %s has no value. Making a new one.',
                         self._rsync_url)
            value = cloud_datastore.entity.Entity(key=self._key)
            value[SyncStatus.COLLECTION_KEY] = u''
            value[SyncStatus.DEBUG_MESSAGE_KEY] = u''
            value[SyncStatus.LAST_COLLECTION_KEY] = u''
            value[SyncStatus.MTIME_KEY] = 0
        value[entry_key] = entry_value
        self._client.put(value)

    def update_last_archived_date(self, date):
        """Updates the date before which it is safe to delete data."""
        date_str = u'x%d-%02d-%02d' % (date.year, date.month, date.day)
        self.update_data(self.COLLECTION_KEY, date_str)

    def update_debug_message(self, message):
        """Updates the debug message in cloud datastore."""
        message = message[:1400]  # Datastore has a 1500 byte max
        self.update_data(self.DEBUG_MESSAGE_KEY, unicode(message, 'UTF-8'))

    def update_last_collection(self):
        """Updates the last collection time in cloud datastore."""
        text = datetime.datetime.utcnow().strftime('x%Y-%02m-%02d-%02H:%02M')
        self.update_data(self.LAST_COLLECTION_KEY, unicode(text, 'UTF-8'))

    def update_mtime(self, mtime):
        """Updates the mtime column in cloud datastore."""
        self.update_data(self.MTIME_KEY, mtime)


class SyncStatusLogHandler(logging.Handler):
    """Handles error log messages by writing them to cloud datastore."""

    def __init__(self, status_storage):
        logging.Handler.__init__(self, level=logging.ERROR)
        self.setFormatter(
            logging.Formatter('[%(asctime)s %(levelname)s '
                              '%(filename)s:%(lineno)d] %(message)s'))
        self._status_storage = status_storage

    def handle(self, record):
        self._status_storage.update_debug_message(self.format(record))

    def emit(self, _record):  # pragma: no cover
        """Abstract in the base class, overwritten to keep the linter happy."""


def init(args):  # pragma: no cover
    """Initialize the scraper library.

    The discovery interface means that the contents of some libraries is
    determined at runtime.  Also, applications need to be authorized to use the
    necessary services.  This performs both library initialization as well as
    application authorization.
    """
    rsync_url = 'rsync://{}:{}/{}'.format(args.rsync_host, args.rsync_port,
                                          args.rsync_module)
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s %(levelname)s %(filename)s:%(lineno)d ' +
        rsync_url + '] %(message)s')
    logging.info('Scraping from %s, putting the results in %s', rsync_url,
                 args.bucket)

    # Authorize this application to use Google APIs.
    creds = gce.AppAssertionCredentials()

    # Set up cloud datastore and its dependencies
    datastore_service = cloud_datastore.Client(
        namespace=args.datastore_namespace)
    status = SyncStatus(datastore_service, rsync_url)
    logging.getLogger().addHandler(SyncStatusLogHandler(status))

    # Set up cloud storage
    storage_service = apiclient.discovery.build(
        'storage', 'v1', credentials=creds)

    # If the destination directory does not exist, make it exist.
    destination = os.path.join(args.data_dir, args.rsync_host,
                               args.rsync_module)
    if not os.path.isdir(destination):
        os.makedirs(destination)
    return (rsync_url, status, destination, storage_service)


def download(args, rsync_url, sync_status, destination):  # pragma: no cover
    """Rsync download all files that are new enough.

    Find the current last_archived_date from cloud datastore, then get the file
    list and download the files from the server.
    """
    sync_status.update_last_collection()
    last_archived_date = sync_status.get_last_archived_date()
    all_remote_files = list_rsync_files(args.rsync_binary, rsync_url,
                                        destination)
    newer_files = remove_older_files(last_archived_date, all_remote_files)
    download_files(args.rsync_binary, rsync_url, newer_files, destination)


def upload_if_allowed(args, sync_status, destination, storage_service):
    """If enough time has passed, upload old data to GCS."""
    upload_up_to_date(args, sync_status, destination, storage_service,
                      max_new_archived_date())


def upload_stale_disk(args, sync_status, destination, storage_service):
    """Upload all data from the disk where we also have the next day's data."""
    days = list(find_all_days_to_upload(destination, max_new_archived_date()))
    if len(days) <= 1:
        logging.info('No stale data found')
        return
    else:
        days.pop()  # The last day is not okay
        last_okay_day = days[-1]
        logging.warning('Stale data found: %s', str(last_okay_day))
        upload_up_to_date(args, sync_status, destination, storage_service,
                          last_okay_day)


def upload_up_to_date(args, sync_status, destination,
                      storage_service,
                      candidate_last_archived_date):  # pragma: no cover
    """Tar and upload local data.

    Tar up what we have for each unarchived day that is sufficiently in the past
    (up to and including the candidate_last_archived_date), upload what we have,
    and delete the local copies of all successfully-uploaded data.
    """
    max_mtime = None
    for day in find_all_days_to_upload(destination,
                                       candidate_last_archived_date):
        max_mtime = None
        tarfile_template = create_tarfilename_template(day, args.rsync_host,
                                                       args.rsync_module,
                                                       args.tarfile_directory)
        for tgz_filename, max_mtime, num_files in create_temporary_tarfiles(
                args.tar_binary, tarfile_template, destination, day,
                args.max_uncompressed_size):
            upload_tarfile(storage_service, tgz_filename, day,
                           args.rsync_module, args.bucket)
            FILES_UPLOADED.labels(bucket=args.bucket).inc(num_files)
            BYTES_UPLOADED.labels(bucket=args.bucket).inc(
                os.stat(tgz_filename).st_size)
        sync_status.update_last_archived_date(day)
        if max_mtime is not None:
            sync_status.update_mtime(max_mtime)
        remove_datafiles(destination, day)
    if max_mtime is None:
        # The last archived date indicates the date with which we are finished.
        # Therefore, the high water mark should be equal to the last possible
        # high water mark of the day, assuming there was no data that day.
        datetime_value = datetime.datetime(candidate_last_archived_date.year,
                                           candidate_last_archived_date.month,
                                           candidate_last_archived_date.day)
        sync_status.update_mtime(
            datetime_to_epoch(datetime_value +
                              datetime.timedelta(hours=23, minutes=59,
                                                 seconds=59)))
    sync_status.update_debug_message('')
