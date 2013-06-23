#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import datetime
import urllib2
import logging
import sys
import time

import requests
from requests_oauthlib import OAuth1
import json

import db

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
screen_handler = logging.StreamHandler(sys.stderr)
screen_handler.setLevel(logging.INFO)
screen_handler.setFormatter(formatter)
logger.addHandler(screen_handler)


# TODO use logging module
# TODO move OAUTH parameters to external config file (json, YAML or conf)

DESCRIPTION = """Download tweets in realtime using the Twitter Streaming API.

"""

EPILOG = """Requires Python Requests and Python Requests Oauthlib."""

URL = 'https://stream.twitter.com/1.1/statuses/{0}.json'

class TwitterStreamCrawler(object):
    def __init__(self, base_filename, user_key, user_secret, app_key,
            app_secret):
        self.base_filename = base_filename
        self._db_instance = db.FileAppendDb(base_filename)
        self._auth = OAuth1(app_key, app_secret, user_key, user_secret, 
                      signature_type='auth_header')
        file_handler = logging.FileHandler(base_filename+".log")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    def each_tweet(self, line):
        line = line.strip()
        if line:
            self._db_instance.save(line)
            #self._db_instance.sync()
            try:
                tweet = json.loads(line)
                if 'limit' in tweet:
                    n_tweets = tweet['limit']['track']
                    logger.warning("LIMIT: %s tweets retained", n_tweets)
            except ValueError:
                logger.error("Not valid JSON on line: %s", line)

    def request_stream(self, url, data):
        count = 0
        start_time = datetime.datetime.now()
        db_instance = self._db_instance
        r = requests.post(url, data=data, auth=self._auth, stream=True, timeout=self.timeout)
        each_tweet = self.each_tweet
        for line in r.iter_lines():
            each_tweet(line)
            count += 1
            if not count % 100:
                now = datetime.datetime.now()
                delta = now - start_time
                rate = float(count) / delta.total_seconds()
                print("Total tweets", count, "\tRate", rate) # Change to logger.
                start_time = datetime.datetime.now()
                count = 0

    def receive(self, endpoint, data, print_tweets=False):
        url = URL.format(endpoint)
        while True:
            try:
                logger.info("Requesting stream: %s. Params: %s", url, data)
                self.request_stream(url, data)
                # TODO print_tweets
            except (KeyboardInterrupt, SystemExit):
                # TODO clean outfile?
                logger.info("Shutting down (manual shutdown).")
                logging.shutdown()
                sys.exit(0)
            # Handle network timeout
            except requests.exceptions.Timeout as e:
                timeout = self.timeout
                delay = self.delay # TODO introduce exp backoff
                logger.info("Request timed out (timeout=%s). Waiting and retrying (delay=%s).", timeout, delay)
                time.sleep(delay)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""Download twitter streams using the
    Streaming API""", epilog=EPILOG)
    parser.add_argument('ck', help='consumer key')
    parser.add_argument('cs', help='consumer secret')
    parser.add_argument('uk', help='user key')
    parser.add_argument('us', help='user secret')
    parser.add_argument('endpoint', help='method of the Streaming API to use', default='filter',
            choices=('filter', 'sample', 'firehose'))
    parser.add_argument('-p', help="""add a method parameter ('name=value')""",
            metavar="PARNAME=PARVAL", action='append')
    parser.add_argument('-o', '--print', help='print every tweet', action='store_true')
    parser.add_argument('-f', '--file', help='output json to the specified file',
            action='store')
    parser.add_argument('-r', '--rotate',
        help='rotate output file every N hours (default 24)', default=24,
        action='store', metavar="N", type=int)
    parser.add_argument('--timeout', default=30,
        help='Streaming timeout in seconds (default 30)',
        action='store', type=int, metavar="SECS")
    parser.add_argument('--delay', default=10,
        help='Sleep delay in seconds (default 10)',
        action='store', type=int, metavar="SECS")
    args = parser.parse_args()

    endpoint = args.endpoint
    data = {}
    if args.p:
        data = dict([i.split('=') for i in args.p])
    if endpoint == 'filter':
        assert set(data).intersection(('track', 'locations', 'follow'))

    filename = args.file
    api = TwitterStreamCrawler(filename, args.uk, args.us, args.ck, args.cs) 
    api.timeout = args.timeout
    api.delay = args.delay
    api.receive(endpoint, data, print_tweets=True)
