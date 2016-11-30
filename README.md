[![Build Status](https://travis-ci.org/m-lab/signal-searcher.svg?branch=master)](https://travis-ci.org/m-lab/signal-searcher)
[![Coverage Status](https://coveralls.io/repos/github/m-lab/scraper/badge.svg?branch=master)](https://coveralls.io/github/m-lab/scraper?branch=master)

# Scraper
Scrape experiment data off of MLab nodes and upload it to the ETL pipeline.

# Tests
This repo is fully integrated with Travis, but with one wrinkle.  The
end-to-end test can't (currently) be run on Travis.  So the end-to-end test has
been included as a requirement of the pre-commit git hook.  This means that you
can only develop scraper code in a GCE instance.
