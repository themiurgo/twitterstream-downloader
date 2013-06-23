#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import datetime
import urllib2
import sys
import time

import requests
from requests_oauthlib import OAuth1
import json

import db

# TODO use logging module
# TODO move OAUTH parameters to external config file (json, YAML or conf)

DESCRIPTION = """Download tweets in realtime using the Twitter Streaming API.

"""

EPILOG = """Requires Python Requests and Python Requests Oauthlib."""

class TwitterStreamCrawler(object):
    def __init__(self, base_filename, user_key, user_secret, app_key,
            app_secret):
        self.base_filename = base_filename
        self._db_instance = db.FileAppendDb(base_filename)
        self._auth = OAuth1(app_key, app_secret, user_key, user_secret, 
                      signature_type='auth_header')

    def each_tweet(self, line):
        tweet = json.loads(line)
        if 'limit' in tweet:
            now = datetime.datetime.now()
            n_tweets = tweet['limit']['track']
            with open(self.base_filename+".error.log", "a+") as f:
                f.write("LIMIT hit at {0}, {1} tweets retained\n".format(now, n_tweets))
            print("LIMIT hit at {0}, {1} tweets retained".format(now, n_tweets))
        self._db_instance.save(line)
        #self._db_instance.sync()

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
                print("Total tweets", count, "\tRate", rate)
                start_time = datetime.datetime.now()
                count = 0

    def receive(self, endpoint, data, print_tweets=False):
        # TODO print_tweets
        url = 'https://stream.twitter.com/1.1/statuses/{0}.json'.format(endpoint)
        while True:
            try:
                self.request_stream(url, data)
            except KeyboardInterrupt:
                sys.exit(0)
            except requests.exceptions.Timeout: # Handle timeouts
                with open(self.base_filename+".error.log", "a+") as f:
                    f.write("TIMEOUT at {0}\n".format(datetime.datetime.now()))
                print("TIMEOUT at {0}".format(datetime.datetime.now()))
                print("Retrying in {0} seconds".format(self.delay))
                time.sleep(self.delay) # TODO introduce exp backoff

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""Download twitter streams using the
    Streaming API""", epilog=EPILOG)
    parser.add_argument('ck', help='consumer key')
    parser.add_argument('cs', help='consumer secret')
    parser.add_argument('uk', help='user key')
    parser.add_argument('us', help='user secret')
    parser.add_argument('endpoint', help='method of the Streaming API to use', default='filter')
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
