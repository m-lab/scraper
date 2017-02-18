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

This is a single-shot program to download data from an MLab node and then upload
it to Google Cloud Storage.  It is expected that this program will be called
repeatedly and that there will be many such instances of this program running
simultaneously on one or more machines.

This program expects to be run on GCE and uses both cloud APIs and the Google
Sheets API.  Sheets API access is not enabled by default for GCE, and it can't
be enabled from the web-based GCE instance creation interface.  Worse, the
scopes that a GCE instance has can't be changed after creation. To create a new
GCE instance named scraper-dev that has access to both cloud APIs and
spreadsheet apis, you need to use the following command line:

   gcloud compute instances create scraper-dev \
       --scopes cloud-platform,https://www.googleapis.com/auth/spreadsheets
"""

import collections
import contextlib
import datetime
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile

import apiclient
import httplib2

from oauth2client.contrib import gce


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


# Use IPv4, compression, and limit total bandwidth usage to 10 Mbps
RSYNC_ARGS = ['-4', '-z', '--bwlimit', '10000', '--times']


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
        command = [rsync_binary, '--list-only', '--recursive'] + \
            RSYNC_ARGS + [rsync_url]
        logging.info('Listing files on server with the command: %s',
                     ' '.join(command))
        lines = subprocess.check_output(command).splitlines()
        files = []
        for line in lines:
            # None is a special whitespace arg for split
            chunks = line.split(None, 4)
            if chunks[0].startswith('d'):
                continue
            if len(chunks) != 5:
                logging.error('Bad line in output: %s', line)
                continue
            files.append(chunks[4])
        return files
    except subprocess.CalledProcessError as error:
        logging.error('rsync file listing failed: %s', str(error))
        sys.exit(1)


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
    # Rsync all the files that are new enough for us to care about.
    with tempfile.NamedTemporaryFile() as temp:
        # Write the list of files to a tempfile, so as not to have to worry
        # about too-long command lines full of filenames.
        for fname in files:
            print >> temp, fname
        temp.flush()
        if os.stat(temp.name).st_size == 0:
            logging.warning('No files to be downloaded from %s', rsync_url)
            return
        # Download all the files.
        try:
            logging.info('Synching %d files', len(files))
            command = ([rsync_binary, '--files-from', temp.name] + RSYNC_ARGS +
                       [rsync_url, destination])
            subprocess.check_call(command)
        except subprocess.CalledProcessError as error:
            logging.error('rsync download failed: %s', str(error))
            sys.exit(1)
        logging.info('sync completed successfully from %s', rsync_url)


def max_new_high_water_mark():
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

    Returns the host and site contained in the hostname of the mlab node. Strips
    .measurement-lab.org from the hostname if it exists. Existing files have
    names like 20150706T000000Z-mlab1-acc01-ndt-0000.tgz and this function is
    designed to return the pair ('mlab1', 'acc01') as derived from a hostname
    like 'ndt.iupui.mlab2.nuq1t.measurement-lab.org'
    """
    assert_mlab_hostname(host)
    names = host.split('.')
    return (names[-4], names[-3])


def attempt_decompression(gunzip_binary, filename):
    """Attempt to decompress a .gz file.

    If the attempt fails, return the original filename. Otherwise return the new
    filename.
    """
    assert filename.endswith('.gz'), 'Bad filename to decompress: ' + filename
    basename = filename[:-3]
    if os.path.exists(basename):
        logging.warning(
            'Assuming that already-existing file %s is the unzipped '
            'version of %s', basename, filename)
        return basename
    command = [gunzip_binary, filename]
    try:
        subprocess.check_call(command)
    except subprocess.CalledProcessError as error:
        logging.error('gunzip failed on %s (%s)', filename, error.message)
        return filename
    if not os.path.exists(basename):
        logging.error('gunzip of %s failed to create %s', filename,
                      basename)
        return filename
    return basename


def create_temporary_tarfiles(tar_binary, gunzip_binary, directory, day, host,
                              experiment, max_uncompressed_size):
    """Create tarfiles, and yield the name of each tarfile as it is made.

    Because one day may contain a lot of data, we create a series of tarfiles,
    none of which may contain more than max_uncompressed_size buytes of data.
    Upon resumption, remove the tarfile that was created.

    Args:
      tar_binary: the full pathname for the tar binary
      directory: the directory at the root of the file hierarchy
      day: the date for the tarfile
      host: the hostname for the tar file
      experiment: the experiment with data contained in this tarfile
      max_uncompressed_size: the max size of an individual tarfile

    Yields:
      A tuple of the name of the tarfile created and the most recent mtime of
      any tarfile's component files
    """
    node, site = node_and_site(host)
    # Ensure that the filenames we generate match the existing files that have
    # names like '20150706T000000Z-mlab1-acc01-ndt-0000.tgz'.
    filename_prefix = '%d%02d%02dT000000Z-%s-%s-%s-' % (
        day.year, day.month, day.day, node, site, experiment)
    filename_suffix = '.tgz'
    day_dir = '%d/%02d/%02d' % (day.year, day.month, day.day)
    tarfile_size = 0
    tarfile_files = []
    tarfile_index = 0
    max_mtime = 0
    with chdir(directory):
        for filename in sorted(os.listdir(day_dir)):
            filename = os.path.join(day_dir, filename)
            # TODO(https://github.com/m-lab/scraper/issues/7) compression
            # Stop with this compression and decompression nonsense by deleting
            # all code between this comment and the one that says "END TODO"
            if filename.endswith('.gz'):
                filename = attempt_decompression(gunzip_binary, filename)
            # END TODO(https://github.com/m-lab/scraper/issues/7)
            filestat = os.stat(filename)
            filesize = filestat.st_size
            max_mtime = max(max_mtime, int(filestat.st_mtime))
            if (tarfile_files and
                    tarfile_size + filesize > max_uncompressed_size):
                tarfile_name = '%s%04d%s' % (filename_prefix, tarfile_index,
                                             filename_suffix)
                try:
                    create_tarfile(tar_binary, tarfile_name, tarfile_files)
                    logging.info('Created %s', tarfile_name)
                    yield tarfile_name, max_mtime
                finally:
                    logging.info('removing %s', tarfile_name)
                    os.remove(tarfile_name)
                tarfile_files = []
                tarfile_size = 0
                tarfile_index += 1
            tarfile_files.append(filename)
            tarfile_size += filesize
        if tarfile_files:
            tarfile_name = '%s%04d%s' % (filename_prefix, tarfile_index,
                                         filename_suffix)
            try:
                create_tarfile(tar_binary, tarfile_name, tarfile_files)
                logging.info('Created %s', tarfile_name)
                yield tarfile_name, max_mtime
            finally:
                logging.info('removing %s', tarfile_name)
                os.remove(tarfile_name)


def upload_tarfile(service, tgz_filename, date, bucket):  # pragma: no cover
    """Uploads a tarfile to Google Cloud Storage for later processing.

    Puts the file into a GCS bucket. If a file of that same name already
    exists, the file is overwritten.

    Args:
      service: the service object returned from discovery
      tgz_filename: the basename of the tarfile
      date: the date for the data
      bucket: the name of the GCS bucket
    """
    name = '%d/%02d/%02d/%s' % (date.year, date.month, date.day, tgz_filename)
    body = {'name': name}
    # Upload in 10 meg chunks
    media = apiclient.http.MediaFileUpload(tgz_filename,
                                           chunksize=10 * 1025 * 1024,
                                           resumable=True)
    logging.info('Uploading %s to %s/%s', tgz_filename, bucket, body['name'])
    request = service.objects().insert(
        bucket=bucket, body=body, media_body=media)
    response = None
    while response is None:
        progress, response = request.next_chunk()
        logging.debug('Uploaded %d%%', 100.0 * progress.progress())
    logging.info('Upload to %s/%s complete!', bucket, body['name'])


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


def cell_to_date_or_die(cell_text):
    """Converts a cell of the form 'x2016-01-28' into a date."""
    try:
        assert cell_text.count('-') == 2 and cell_text[0] == 'x'
        year, month, day = cell_text[1:].split('-')
        assert year.isdigit() and month.isdigit() and day.isdigit()
        return datetime.date(int(year, 10), int(month, 10), int(day, 10))
    except (AssertionError, ValueError):
        logging.error('Bad spreadsheet cell for the date: "%s"', cell_text)
        sys.exit(1)


class Spreadsheet(object):
    """A Spreadsheet retrieves and updates the contents of a Google sheet."""

    RSYNC_COLUMN = 'dropboxrsyncaddress'
    COLLECTION_COLUMN = 'lastsuccessfulcollection'
    DEBUG_MESSAGE_COLUMN = 'errorsincelastsuccessful'
    LAST_COLLECTION_COLUMN = 'lastcollectionattempt'
    MTIME_COLUMN = 'maxrawfilemtimearchived'

    def __init__(self, service, spreadsheet,
                 worksheet='Drop box status (auto updated)',
                 default_range='A:F'):
        self._service = service
        self._spreadsheet = spreadsheet
        self._worksheet = worksheet
        self._default_range = default_range

    def get_data(self, worksheet_range=None):  # pragma: no cover
        """Retrieves data from a spreadsheet.

        A separate function so that it can be mocked for testing purposes.
        """
        if worksheet_range is None:
            worksheet_range = self._default_range
        sheet_range = self._worksheet + '!' + worksheet_range
        result = self._service.spreadsheets().values().get(
            spreadsheetId=self._spreadsheet, range=sheet_range).execute()
        return result.get('values', [])

    def get_progress(self, rsync_url, default_date=datetime.date(2009, 1, 1)):
        """Returns the most recent date from which we have all the data.

        Downloads everything in the spreadsheet, then finds the right row, and
        then the right column in that row, and returns the date stored at that
        cell.

        Args:
          rsync_url: the url to download from (determines the row)
          default_date: the time to return if the row does not exist
        """
        values = self.get_data()
        if not values:
            logging.critical('No data found in the given spreadsheet')
            sys.exit(1)
        header = values[0]
        rsync_index = header.index(self.RSYNC_COLUMN)
        date_index = header.index(self.COLLECTION_COLUMN)
        for row in values[1:]:
            if row[rsync_index] == rsync_url:
                date_str = row[date_index]
                if not date_str or not date_str.strip():
                    return default_date
                logging.info('Old high water mark was %s', date_str)
                return cell_to_date_or_die(date_str)
        logging.warning('No row found for %s', rsync_url)
        return default_date

    def update_data(self, rsync_url, column, value):  # pragma: no cover
        """Updates a single cell on the spreadsheet.

        The row and column of the cell are determined by the rsync_url and
        column values, respectively.  If no such row exists, then one will be
        created.

        Args:
          rsync_url: determines the row
          column: determines the column (must be one of the header values)
          value: the new value to write to the cell
        """
        values = self.get_data()
        if not values:
            logging.critical('No data found in the given spreadsheet')
            sys.exit(1)
        header = values[0]
        rsync_index = header.index(self.RSYNC_COLUMN)
        column_index = header.index(column)
        assert column_index <= 26, 'Too many columns'
        for row_index in range(1, len(values)):
            if values[row_index][rsync_index] == rsync_url:
                # Convert zero-based index stored in row_index to one-based
                # spreadsheet row index.
                cell_id = chr(ord('A') + column_index) + str(row_index + 1)
                update_range = self._worksheet + '!' + cell_id
                values = [[value]]
                body = {'values': values}
                logging.info('About to update %s (%s, %s) to %s', cell_id,
                             rsync_url, column, values[0][0])
                response = self._service.spreadsheets().values().update(
                    spreadsheetId=self._spreadsheet, range=update_range,
                    body=body, valueInputOption='RAW').execute()
                assert response['updatedRows'], 'Bad update ' + str(response)
                return
        # Append a new row.
        data = collections.defaultdict(str)
        data[self.RSYNC_COLUMN] = rsync_url
        data[column] = value
        new_row = [data[x] for x in header]
        body = {'values': [new_row]}
        range_name = self._worksheet + '!' + self._default_range
        response = self._service.spreadsheets().values().append(
            spreadsheetId=self._spreadsheet, range=range_name, body=body,
            valueInputOption='RAW').execute()
        assert response['updates']['updatedRows'], 'Bad append ' + str(response)

    def update_high_water_mark(self, rsync_url, date):
        """Updates the date before which it is safe to delete data."""
        date_str = 'x%d-%02d-%02d' % (date.year, date.month, date.day)
        self.update_data(rsync_url, self.COLLECTION_COLUMN, date_str)

    def update_debug_message(self, rsync_url, message):
        """Updates the debug message on the spreadsheet."""
        self.update_data(rsync_url, self.DEBUG_MESSAGE_COLUMN, message)

    def update_last_collection(self, rsync_url):
        """Updates the last collection time on the spreadsheet."""
        text = datetime.datetime.utcnow().strftime('x%Y-%02m-%02d-%02H:%02M')
        self.update_data(rsync_url, self.LAST_COLLECTION_COLUMN, text)

    def update_mtime(self, rsync_url, mtime):
        """Updates the mtime column on the spreadsheet."""
        self.update_data(rsync_url, self.MTIME_COLUMN, mtime)


class SpreadsheetLogHandler(logging.Handler):
    """Handles error log messages by writing them to the shared spreadsheet."""

    def __init__(self, rsync_url, spreadsheet):
        logging.Handler.__init__(self, level=logging.ERROR)
        self.setFormatter(
            logging.Formatter('[%(asctime)s %(levelname)s '
                              '%(filename)s:%(lineno)d] %(message)s'))
        self._rsync_url = rsync_url
        self._sheet = spreadsheet

    def handle(self, record):
        self._sheet.update_debug_message(self._rsync_url, self.format(record))

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
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s %(levelname)s %(filename)s:%(lineno)d ' +
        rsync_url + '] %(message)s')

    logging.info('Scraping from %s, putting the results in %s', rsync_url,
                 args.bucket)

    # Authorize this application to use Google APIs.
    creds = gce.AppAssertionCredentials()

    # Set up the Sheets and Cloud Storage APIs.
    http = creds.authorize(httplib2.Http())
    discovery_url = ('https://sheets.googleapis.com/$discovery/rest?'
                     'version=v4')
    sheets_service = apiclient.discovery.build(
        'sheets', 'v4', http=http, discoveryServiceUrl=discovery_url)
    spreadsheet = Spreadsheet(sheets_service, args.spreadsheet)
    storage_service = apiclient.discovery.build(
        'storage', 'v1', credentials=creds)

    spreadsheet.update_last_collection(rsync_url)
    logging.getLogger().addHandler(
        SpreadsheetLogHandler(rsync_url, spreadsheet))

    # If the destination directory does not exist, make it exist.
    destination = os.path.join(args.data_dir, args.rsync_host,
                               args.rsync_module)
    if not os.path.isdir(destination):
        os.makedirs(destination)
    return (rsync_url, spreadsheet, destination, storage_service)


def download(rsync_binary, rsync_url, spreadsheet,
             destination):  # pragma: no cover
    """Rsync download all files that are new enough.

    Find the current progress level from the spreadsheet, then get the file list
    and download the files from the server.
    """
    progress_level = spreadsheet.get_progress(rsync_url)
    all_files = list_rsync_files(rsync_binary, rsync_url)
    newer_files = remove_older_files(progress_level, all_files)
    download_files(rsync_binary, rsync_url, newer_files, destination)


def upload_if_allowed(args, rsync_url, spreadsheet, destination,
                      storage_service):  # pragma: no cover
    """If enough time has passed, upload old data to GCS.

    Tar up what we have for each un-uploaded day that is sufficiently in the
    past (up to and including the new high water mark), upload what we have, and
    delete the local copies of all successfully-uploaded data.
    """
    new_high_water_mark = max_new_high_water_mark()
    for day in find_all_days_to_upload(destination, new_high_water_mark):
        for tgz_filename, mtime in create_temporary_tarfiles(
                args.tar_binary, args.gunzip_binary, destination, day,
                args.rsync_host, args.rsync_module, args.max_uncompressed_size):
            upload_tarfile(storage_service, tgz_filename, day, args.bucket)
            spreadsheet.update_mtime(rsync_url, mtime)
        spreadsheet.update_high_water_mark(rsync_url, day)
        remove_datafiles(destination, day)
    spreadsheet.update_debug_message(rsync_url, '')
