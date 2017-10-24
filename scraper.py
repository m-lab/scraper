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

import collections
import contextlib
import datetime
import logging
import os
import re
import subprocess
import tempfile

import apiclient
import googleapiclient.errors
import prometheus_client
import retry

from oauth2client.contrib import gce

# pylint: disable=no-name-in-module
import google.cloud.datastore as cloud_datastore
# pylint: enable=no-name-in-module


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
    'Total file count of the test files uploaded to GCS',
    ['rsync_host_module', 'day_of_week'])
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


def has_one_bit_set_or_is_zero(i):
    """Returns true if the number has one bit set or is zero.

    Adapted from https://goo.gl/mGdFkS but could also be from "Hacker's
    Delight".
    """
    return (i & (i - 1)) == 0


# Use IPv4, archive mode, compression, limit total bandwidth usage to 10 Mbps,
# don't wait too long before bailing out, and make sure to chmod the files to
# have sensible permissions.
RSYNC_ARGS = ['-4', '-az', '--bwlimit=10000', '--timeout=300',
              '--contimeout=300', '--chmod=u=rwX']


RemoteFile = collections.namedtuple('RemoteFile', ['filename', 'mtime'])


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
      destination: the directory to download to

    Returns:
      a list of RemoteFile objects

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
    # pylint: disable=line-too-long
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
    # 2017/10/12/20171012T22:09:14.480679000Z_mobile-166-172-63-50.mycingular.net:52559.meta is uptodate
    # 2017/10/12/20171012T22:09:18.978633000Z_24-151-108-33.dhcp.nwtn.ct.charter.com:48820.cputime.gz is uptodate
    # 2017/10/12/20171012T22:09:18.978633000Z_24-151-108-33.dhcp.nwtn.ct.charter.com:48820.meta is uptodate
    # 2017/10/12/20171012T22:09:18.978633000Z_24-151-108-33.dhcp.nwtn.ct.charter.com:59812.s2c_snaplog.gz is uptodate
    # [receiver] expand file_list pointer array to 524288 bytes, did move
    # [generator] expand file_list pointer array to 524288 bytes, did move
    # 2017/10/12/ 2017/10/13-08:51:08
    # 2017/10/12/20171012T22:09:18.978633000Z_24.151.102017/10/12/20171012T22:09:14.480679000Z_mobile-166-172-63-50.mycingular.net:41160.c2s_snaplog.gz is uptodate
    # 2017/10/12/20171012T22:09:24.837734000Z_24.152.248.171.c2s_ndttrace.gz 2017/10/12-22:09:38
    # 2017/10/12/20171012T22:09:24.837734000Z_24.152.248.171.res-cmts.tvh.ptd.net:55683.cputime.gz 2017/10/12-22:09:38
    # 2017/10/12/20171012T22:09:24.837734000Z_24.152.248.171.res-cmts.tvh.ptd.net:55683.meta 2017/10/12-22:09:38
    # [snip]
    # The lines with [generator] and [receiver] may happen at any point in the
    # output.
    #
    # pylint: enable=line-too-long

    # -n causes the whole thing to run in dry-run mode
    # -vv causes the debug output which we parse
    # -out-format causes the output to be the filename, then a space, then the
    #             mtime of the file in question.
    command = ([rsync_binary, '-n', '-vv', '--out-format', '%n %M'] +
               RSYNC_ARGS +
               [rsync_url, destination])
    logging.info('Listing files on server with the command: %s',
                 ' '.join(command))
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    files = []
    # Only download things that are files and that respect the date-based
    # directory structure, on lines that end with a conforming timestamp.
    timestamp_re_str = r'\d{4}/\d\d/\d\d-\d\d:\d\d:\d\d'
    files_regex = re.compile(r'^\d{4}/\d\d/\d\d/.*[^/]' + ' ' +
                             timestamp_re_str +
                             '$')
    line_count = 0
    for line in process.stdout:
        # Count output lines independently from files found.
        line_count += 1
        if has_one_bit_set_or_is_zero(line_count):
            logging.info('%d lines of rsync output read', line_count)
        # Get rid of the trailing newline.
        line = line.strip()
        # Don't re-sync files that are already in sync.
        if line.endswith(' is uptodate'):
            continue
        # If it looks like a file and isn't uptodate, add it to our list.
        if files_regex.match(line):
            # The split and strptime are safe because the line matched the
            # files_regex.
            filename, timestamp_str = line.rsplit(' ', 1)
            timestamp = datetime.datetime.strptime(timestamp_str,
                                                   '%Y/%m/%d-%H:%M:%S')
            files.append(RemoteFile(filename, timestamp))
            # Logging that decreases exponentially over time.
            if has_one_bit_set_or_is_zero(len(files)):
                logging.info('Found %d files to download so far', len(files))
        else:
            logging.debug('LINE does not match %s: "%s"',
                          files_regex.pattern, line)
    logging.info('Found %d files to download in total', len(files))
    process.wait()
    logging.info('rsync process exited with code %d', process.returncode)
    # Return code 24 from rsync is "partial transfer because some files
    # disappeared", which is totally fine with us - ephemeral files disappearing
    # is no cause for alarm.
    # Return code 23 is "partial transfer for other unknown reasons", which also
    # can occur when files disappear on the server side.
    # Neither return code should cause the listing to error out.
    if process.returncode not in (0, 23, 24):
        message = 'rsync file listing failed (%d): %s' % (process.returncode,
                                                          process.stderr.read())
        logging.error(message)
        raise RecoverableScraperException('rsync_listing', message)
    return files


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
      files: an iterable of RemoteFile objects to retrieve
      destination: the directory on the local host to put the files
    """
    # Dates are no longer needed, and we need to iterate over the sequence of
    # filenames multiple times.
    files = [remote.filename for remote in files]
    if not files:
        logging.info('No files to be downloaded from %s', rsync_url)
        return
    # Rsync all the files passed in.  Do this piecewise, because rsync allocates
    # a per-file chunk of memory, so long file lists end up causing huge memory
    # usage.
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
                # Run rsync.
                # Use all the default arguments.
                # Don't crash when ephemeral files disappear.
                # Filenames in the temp file are null-separated.
                # The filenames to transfer are in a file.
                command = ([rsync_binary] + RSYNC_ARGS + ['--from0',
                                                          '--files-from',
                                                          temp.name, rsync_url,
                                                          destination])
                error_code = subprocess.call(command)
                if error_code not in (0, 24):
                    message = 'rsync download failed exit code: %d' % error_code
                    logging.error(message)
                    raise RecoverableScraperException('rsync_download', message)
    logging.info('sync completed successfully from %s', rsync_url)


def max_new_archived_datetime():
    """The most recent datetime that we could consider "old enough" to upload.

    8 hours after midnight, we will assume that no tests from the previous day
    could possibly have failed to be written to disk.  So this should always be
    one second before midnight either yesterday or the day before, depending on
    how late in the day it is.

    Returns:
        The most recent datetime before which data is safe to upload.
    """
    day = (datetime.datetime.utcnow() - datetime.timedelta(
        days=1, hours=8)).date()
    return datetime.datetime(day.year, day.month, day.day, 23, 59, 59)


def datetime_to_epoch(datetime_value):
    """Converts a datetime value into seconds since epoch.

    This should be a member function of the datetime class, but they did not see
    fit to provide this functionality.
    """
    epoch = datetime.datetime(year=1970, month=1, day=1)
    return int((datetime_value - epoch).total_seconds())


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


LocalBufferedFile = collections.namedtuple('LocalBufferedFile',
                                           ['filename', 'mtime', 'size'])


def all_files(directory, high_water_mark, too_recent_timestamp):
    """Lists all files and mtimes in all subdirectories.

    Ensures that the mtime of the file is between the two timestamps.

    Yields:
        a sequence of LocalBufferedFile objects
    """
    high_water_mark = datetime_to_epoch(high_water_mark)
    too_recent_timestamp = datetime_to_epoch(too_recent_timestamp)
    for root, _dirs, files in os.walk(directory):
        for filename in files:
            fullname = os.path.join(root, filename)
            if fullname.startswith('./'):
                fullname = fullname[2:]
            stat = os.stat(fullname)
            mtime = stat.st_mtime
            size = stat.st_size
            if high_water_mark < mtime <= too_recent_timestamp:
                yield LocalBufferedFile(fullname, mtime, size)


class TarfileTemplate(object):
    """A template for tarfile filenames.

    Each tarfile name should uniquely identify the mlab node, experiment, and
    timestamp for the contained data.  Changes to the existing name scructure
    should be coordinated with the pipeline code.
    """

    def __init__(self, tarfile_directory, node, site, experiment):
        self.tarfile_directory = tarfile_directory
        self.node = node
        self.site = site
        self.experiment = experiment

    def create_filename(self, mtime):
        """Create a filename for a particular time using the template.

        The -0000 suffix to the basename has been left in as a thing to keep
        during the transition period from daily scraper to a more frequent one.
        This code assumes that no two files with the same timestamp will ever
        end up in separate tarfiles, which is a restriction that
        create_temporary_tarfiles enforces.
        """
        mtime = datetime.datetime.utcfromtimestamp(mtime)
        return ('{directory}/{year:04d}{month:02d}{day:02d}T'
                '{hour:02d}{minute:02d}{second:02d}Z'
                '-{node}-{site}-{experiment}-0000.tgz').format(
                    directory=self.tarfile_directory,
                    year=mtime.year, month=mtime.month, day=mtime.day,
                    hour=mtime.hour, minute=mtime.minute, second=mtime.second,
                    node=self.node, site=self.site, experiment=self.experiment)


def create_temporary_tarfiles(tar_binary, tarfile_template, directory,
                              early_time, late_time, max_uncompressed_size):
    """Create tarfiles, and yield the name of each tarfile as it is made.

    Creates appropriately-sized tarfiles for each time period.  All files with
    the same timestamp will be put in the same tarfile, so the tarfiles may be
    bigger than the max_uncompressed_size if more than max_uncompressed_size
    data is written in a single second.

    It is difficult to imagine more than max_uncompressed_size bytes of data
    being written to disk every second.  Each NDT test seems to cause a few (3?
    5?) megabytes of data to be written.  An NDT testing rate that causes in
    excess of 50MB/sec (50MB is the current deployed value) would be 10+ tests
    being initiated every second, which corresponds to more than of 100 tests
    being run simultaneously.  This could happen, but is unlikely.  Even more,
    the number 50MB is chosen pretty roughly - nothing would break if files
    became e.g. 1GB, but the parsing process would run a little less smoothly.
    If we are regularly getting more than 1GB per second, then scraper can't
    keep up with the load, even in the most aggressive "download, upload early,
    and delete" scenario.

    Args:
      tar_binary: the full pathname for the tar binary
      tarfile_template: a string to serve as the tarfile filename template
      directory: the directory at the root of the file hierarchy
      early_time: the time before which we should ignore files
      late_time: the time after which we should ignore files
      max_uncompressed_size: the max size of an individual tarfile

    Yields:
      A tuple of the name of the tarfile created, the oldest mtime of the
      tarfile's component files, the newest mtime of any tarfile's component
      files, and the number of files in the tarfile.  Also, it creates the
      tarfile, and then deletes it after the yield resumes.
    """
    with chdir(directory):
        tarfile_size = 0
        tarfile_files = []
        tarfile_index = 0
        min_mtime = float('inf')
        max_mtime = 0
        prev_timestamp = None

        for local_file in sorted(all_files('.', early_time, late_time),
                                 cmp=lambda x, y: cmp(x.mtime, y.mtime)):
            if (tarfile_files and
                    tarfile_size + local_file.size > max_uncompressed_size and
                    local_file.mtime != prev_timestamp):
                tarfile_name = tarfile_template.create_filename(min_mtime)
                create_tarfile(tar_binary, tarfile_name, tarfile_files)
                logging.info('Created local file %s', tarfile_name)
                yield tarfile_name, min_mtime, max_mtime, len(tarfile_files)
                os.remove(tarfile_name)
                logging.info('Removed local file %s', tarfile_name)
                tarfile_files = []
                tarfile_size = 0
                tarfile_index += 1
                min_mtime = local_file.mtime
            tarfile_files.append(local_file.filename)
            tarfile_size += local_file.size
            prev_timestamp = local_file.mtime
            min_mtime = min(local_file.mtime, min_mtime)
            max_mtime = max(local_file.mtime, max_mtime)
        if tarfile_files:
            tarfile_name = tarfile_template.create_filename(min_mtime)
            create_tarfile(tar_binary, tarfile_name, tarfile_files)
            logging.info('Created local file %s', tarfile_name)
            yield tarfile_name, min_mtime, max_mtime, len(tarfile_files)
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
                   bucket):
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
    except googleapiclient.errors.HttpError as error:  # pragma: no cover
        if (error.resp.status // 100) == 5:  # HTTP 500 is recoverable
            logging.warning('Recoverable error on upload: ' + str(error))
            raise RecoverableScraperException('upload', str(error))
        else:
            logging.warning('Non-recoverable error on upload: ' + str(error))
            raise NonRecoverableScraperException('upload', str(error))


def delete_datafiles_up_to(directory, max_mtime):
    """Removes files with an mtime before a given datetime from the local disk.

    Prunes any empty subdirectories that it creates.
    """
    for root, dirs, files in os.walk(directory, topdown=False):
        # Delete too-old files
        for filename in files:
            fullname = os.path.join(root, filename)
            stat = os.stat(fullname)
            mtime = stat.st_mtime
            if mtime <= max_mtime:
                logging.debug('Removing old file %s', fullname)
                os.remove(fullname)
        # Delete empty directories
        for dirname in dirs:
            fulldir = os.path.join(root, dirname)
            if not os.listdir(fulldir):
                logging.debug('Removing empty directory %s', fulldir)
                os.rmdir(fulldir)


def mtime_to_date_or_die(mtime_text):
    """Convert a spreadsheet cell timestamp to a datetime or die trying."""
    try:
        mtime = int(mtime_text)
        return datetime.datetime.utcfromtimestamp(mtime)
    except ValueError:
        message = 'Bad mtime: "%s"' % mtime_text
        logging.error(message)
        raise NonRecoverableScraperException('bad_mtime', message)


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
    delete max_mtime data.  If a rogue process updated the cloud datastore cell,
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

    def get_last_archived_mtime(
            self, default_datetime=datetime.datetime(2009, 1, 1, 0, 0, 0)):
        """Returns the most recent mtime before which we have all the data.

        Used to determine what local data on a node has been archived and is
        safe to delete, and also what data must be downloaded from a node, and
        what data need not be downloaded.  Other than exceptional recovery
        cases, this quantity must must be monotonically increasing.

        Args:
          default_datetime: the time to return if no datastore entry exists
        """
        data = self.get_data()
        if not data:
            logging.info('No data found in the datastore')
            return default_datetime
        elif (SyncStatus.MTIME_KEY not in data or
              not data[SyncStatus.MTIME_KEY]):
            logging.info('Data in the datastore had no %s, returning %s',
                         SyncStatus.MTIME_KEY, default_datetime)
            return default_datetime
        else:
            return mtime_to_date_or_die(data[SyncStatus.MTIME_KEY])

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

    def on_upload_success(self, mtime_datetime):
        """Updates both the last archived date and the last archived mtime."""
        self.update_mtime(datetime_to_epoch(mtime_datetime))
        self.update_last_archived_date(mtime_datetime)
        self.update_debug_message('')


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


def init(args):
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


# How long ago should a file have been last edited before we should consider
# downloading it.
QUIESCENCE_THRESHOLD = datetime.timedelta(minutes=15)


def download(args, rsync_url, sync_status, destination):
    """Rsync download all files that are new enough but not too new.

    Find the current last_archived_date from cloud datastore, then get the file
    list and download the files from the server.
    """
    sync_status.update_last_collection()
    high_water_mark = sync_status.get_last_archived_mtime()
    too_recent = datetime.datetime.utcnow() - QUIESCENCE_THRESHOLD

    all_remote_files = list_rsync_files(args.rsync_binary, rsync_url,
                                        destination)

    files_to_download = [remote_file for remote_file in all_remote_files
                         if high_water_mark < remote_file.mtime <= too_recent]

    download_files(args.rsync_binary, rsync_url, files_to_download, destination)


def upload_is_recommended(high_water_mark, too_recent_boundary,
                          data_buffer_threshold, directory):
    """Returns whether we have enough data buffered to upload eagerly."""
    total = 0
    for local_file in all_files(directory, high_water_mark,
                                too_recent_boundary):
        total += local_file.size
    return total > data_buffer_threshold


def upload_if_allowed(args, sync_status, destination, storage_service):
    """If enough time or data has accrued, upload.

    Data that is newer than the high water mark will be uploaded either starting
    at 8 am UTC the following day, or earlier than that if there is more data
    than the data buffer threshold that was created at least data wait time in
    the past.

    This function should only be run after a successful download().
    """
    # Check if there is too much data in the relevant time range.
    high_water_mark = sync_status.get_last_archived_mtime()
    most_recent_allowable_mtime = datetime.datetime.now() - args.data_wait_time
    if upload_is_recommended(high_water_mark, most_recent_allowable_mtime,
                             args.data_buffer_threshold, destination):
        logging.info('Uploading early due to data volume')
        upload_up_to_date(args, sync_status, destination, storage_service,
                          most_recent_allowable_mtime)
    else:
        # Even if we don't have too much data, do check if we should upload
        # yesterday's data.
        proposed_new_high_water_mark = max_new_archived_datetime()
        if high_water_mark < proposed_new_high_water_mark:
            upload_up_to_date(args, sync_status, destination, storage_service,
                              max_new_archived_datetime())


def upload_stale_disk(args, sync_status, destination, storage_service):
    """Upload old, uploadable data from the disk if there is a lot of it."""
    high_water_mark = sync_status.get_last_archived_mtime()
    most_recent_allowable_mtime = datetime.datetime.now() - args.data_wait_time
    files = sorted(
        all_files(destination, high_water_mark, most_recent_allowable_mtime),
        cmp=lambda x, y: cmp(x.mtime, y.mtime))
    if not files:
        return
    most_recent_mtime = datetime.datetime.utcfromtimestamp(files[-1].mtime)
    oldest_possible_rsync_run = most_recent_mtime - args.data_wait_time
    if upload_is_recommended(high_water_mark, oldest_possible_rsync_run,
                             args.data_buffer_threshold, destination):
        logging.info('Uploading stale data before rsync')
        upload_up_to_date(args, sync_status, destination, storage_service,
                          oldest_possible_rsync_run)


def day_of_week(day):
    """Turn a datetime.date into a string representing the day of the week.

    Ideally would be a method in datetime.date.
    """
    return ('Monday',
            'Tuesday',
            'Wednesday',
            'Thursday',
            'Friday',
            'Saturday',
            'Sunday')[day.weekday()]


def upload_up_to_date(args, sync_status, destination,
                      storage_service,
                      candidate_last_archived_mtime):
    """Tar and upload local data.

    Tar up what we have for each unarchived day that is sufficiently in the past
    (up to and including the candidate_last_archived_date), upload what we have,
    and delete the local copies of all successfully-uploaded data.
    """
    logging.info('Uploading all data prior to %s',
                 candidate_last_archived_mtime)
    node, site = node_and_site(args.rsync_host)
    tarfile_template = TarfileTemplate(args.tarfile_directory,
                                       node, site, args.rsync_module)
    earliest_time = sync_status.get_last_archived_mtime()
    if candidate_last_archived_mtime < earliest_time:  # pragma: no cover
        logging.warning('high water mark (%s) is higher than the requested '
                        'max mtime (%s)',
                        earliest_time,
                        candidate_last_archived_mtime)
        return
    total_daily_files = 0
    for (tgz_filename,
         min_mtime,
         _max_mtime,
         num_files) in create_temporary_tarfiles(args.tar_binary,
                                                 tarfile_template,
                                                 destination,
                                                 earliest_time,
                                                 candidate_last_archived_mtime,
                                                 args.max_uncompressed_size):
        upload_tarfile(storage_service, tgz_filename,
                       datetime.datetime.utcfromtimestamp(min_mtime),
                       args.rsync_module, args.bucket)
        total_daily_files += num_files
        BYTES_UPLOADED.labels(bucket=args.bucket).inc(
            os.stat(tgz_filename).st_size)
    # The FILES_UPLOADED count should only be incremented once we are
    # confident that we won't re-upload all the files. Therefore, update it
    # immediately before or after we call update_last_archived_date().
    FILES_UPLOADED.labels(
        rsync_host_module='%s-%s-%s' % (node, site, args.rsync_module),
        day_of_week=day_of_week(candidate_last_archived_mtime)).inc(
            total_daily_files)
    sync_status.on_upload_success(candidate_last_archived_mtime)
    delete_datafiles_up_to(destination,
                           datetime_to_epoch(candidate_last_archived_mtime))
