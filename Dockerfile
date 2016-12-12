FROM google/cloud-sdk
MAINTAINER Peter Boothe <pboothe@google.com>
# Install all the standard packages we need
RUN apt-get update && apt-get install -y python-pip rsync tar parallel
# Install all the python requirements
ADD requirements.txt /requirements.txt
RUN pip install -r requirements.txt
# Install scraper
ADD scraper.py /scraper.py
RUN chmod +x /scraper.py
ADD run-scraper.sh /run-scraper.sh
RUN chmod +x run-scraper.sh
# If we want to divide up the fleet between multiple scraper instances, we'll
# need to use environment variables here instead of the file mlab-servers.txt.
ADD mlab-servers.txt /mlab-servers.txt
# All daemons must be started here, along with the job they support.
CMD SHELL=/bin/bash parallel --jobs=`wc -l /mlab-servers.txt | awk '{print $1}'` --line-buffer /run-scraper.sh {} < /mlab-servers.txt
