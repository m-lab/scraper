FROM google/cloud-sdk
MAINTAINER Peter Boothe <pboothe@google.com>
# Install all the standard packages we need
RUN apt-get update && apt-get install -y python-pip rsync tar
# Install all the python requirements
ADD requirements.txt /requirements.txt
RUN pip install -r requirements.txt
# Install scraper
ADD scraper.py /scraper.py
RUN chmod +x /scraper.py
ADD run-scraper.sh /run-scraper.sh
RUN chmod +x run-scraper.sh
## Set up health checking
# ADD check-health.sh /check-health.sh
# RUN chmod +x check-health.sh
# HEALTHCHECK CMD ./check-health.sh || exit 1
# All daemons must be started here, along with the job they support.
CMD /run-scraper.sh $RSYNC_HOST $RSYNC_MODULE
