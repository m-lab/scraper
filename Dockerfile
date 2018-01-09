FROM alpine:3.6
MAINTAINER Peter Boothe <pboothe@google.com>
# Install all the standard packages we need
RUN apk update && apk add python python-dev py2-pip gcc g++ libc-dev bash rsync tar
# Install all the python requirements
ADD requirements.txt /requirements.txt
RUN pip install -r requirements.txt -U
# Install scraper
ADD scraper.py /scraper.py
RUN chmod +x /scraper.py
ADD run_scraper.py /run_scraper.py
RUN chmod +x run_scraper.py
# The monitoring port
EXPOSE 9090
# The :- syntax specifies a default value for the variable, so the deployment
# need not set it unless you want to specify something other than that default.
CMD /run_scraper.py \
    --rsync_host=$RSYNC_HOST \
    --rsync_port=${RSYNC_PORT:-7999} \
    --rsync_module=$RSYNC_MODULE \
    --bucket=$GCS_BUCKET \
    --data_dir=scraper_data \
    --datastore_namespace=$DATASTORE_NAMESPACE \
    --metrics_port=${METRICS_PORT:-9090} \
    --expected_wait_time=${EXPECTED_WAIT_TIME:-1800} \
    --max_uncompressed_size=${MAX_UNCOMPRESSED_SIZE:-1000000000} \
    --tarfile_directory=${TARFILE_DIRECTORY:-/tmp}
