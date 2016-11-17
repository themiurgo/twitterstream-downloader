#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import datetime
import json
import logging
import os.path
import sys
import time

import requests
from requests_oauthlib import OAuth1
from urlparse import parse_qs

import db
from auth import Keychain
from yn import query_yes_no

# Set up logging
LOGGER = logging.getLogger(__name__)

def setup_logging():
    LOGGER.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    screen_handler = logging.StreamHandler(sys.stderr)
    screen_handler.setLevel(logging.INFO)
    screen_handler.setFormatter(formatter)
    LOGGER.addHandler(screen_handler)

def iterjson(lines):
    for line in lines:
        line = line.strip()
        try:
            yield json.loads(line)
        except ValueError:
            if line:
                LOGGER.error("Not valid JSON on line: %s", line)
            else:
                LOGGER.info("Keep-alive message. No new tweets.")


# TODO adapt the timeout to the rate of the tweets
# TODO multiple accounts (oauth)
# TODO multiple config location (home, current dir, ...)
# TODO better recovery (backfill)
# TOOD print tweets

DESCRIPTION = """Download tweets in realtime using the Twitter Streaming API.

"""

EPILOG = """Requires Python Requests and Python Requests Oauthlib."""


S_AUTH1 = """
To authorize the app, please go to {0} and follow the process.
Please enter the PIN to complete the authorization process.
"""
S_AUTH2 = """
Authorization details:
USER_KEY: {0},
USER_SECRET: {1}
"""

def authorize(consumer_key, consumer_secret):
    oauth = OAuth1(consumer_key, client_secret=consumer_secret)
    request_token_url = "https://api.twitter.com/oauth/request_token"
    response = requests.post(url=request_token_url, auth=oauth)
    credentials = parse_qs(response.content)
    resource_owner_key = credentials.get('oauth_token')[0]
    resource_owner_secret = credentials.get('oauth_token_secret')[0]
    authorize_url = "https://api.twitter.com/oauth/authorize?oauth_token="
    authorize_url = authorize_url + resource_owner_key
    print(S_AUTH1.format(authorize_url))
    verifier = raw_input('PIN: ')

    oauth = OAuth1(consumer_key, consumer_secret,
            resource_owner_key, resource_owner_secret, verifier=verifier)
    access_token_url = "https://api.twitter.com/oauth/access_token"
    response = requests.post(url=access_token_url, auth=oauth)
    credentials = parse_qs(response.content)
    access_token = credentials.get('oauth_token')[0]
    access_token_secret = credentials.get('oauth_token_secret')[0]
    #print(S_AUTH2.format(resource_owner_key, resource_owner_secret))
    return access_token, access_token_secret

def make_pipeline(source, pipeline_steps):
    """Make a pipeline."""
    for step in pipeline_steps:
        source = step(source)
    return source

class TwitterStreamCrawler(object):
    url = 'https://stream.twitter.com/1.1/statuses/{0}.json'

    def __init__(self, base_filename, auth):
        self.base_filename = base_filename
        self._db_instance = db.FileAppendDb(base_filename)
        self._auth = auth
        file_handler = logging.FileHandler(base_filename+".log")
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        LOGGER.addHandler(file_handler)

    def _save_to_db(self, lines):
        db_instance = self._db_instance
        for line in lines:
            line = line.strip()
            db_instance.save(line)
            #self._db_instance.sync()
            yield line

    @staticmethod
    def _check_limit(tweets):
        for tweet in tweets:
            if 'limit' in tweet:
                n_tweets = tweet['limit']['track']
                LOGGER.warning("LIMIT: %s tweets retained", n_tweets)
            yield tweet

    def request_stream(self, endpoint, data, timeout):
        """Executes the pipeline and returns an iterator of tweets (dict)."""
        url = self.url.format(endpoint)

        pipeline_steps = [
            self._save_to_db,
            iterjson,
            self._check_limit,
        ]
        request = requests.post(url, data=data, auth=self._auth, stream=True,
                timeout=timeout)
        stream = request.iter_lines()
        pipeline = make_pipeline(stream, pipeline_steps)

        for tweet in pipeline:
            yield tweet


def make_keychain():
    k = Keychain()

    # Try to load a previous keychain
    home = os.path.expanduser("~")
    basename = ".twsd.auth"
    authfname = os.path.join(home, basename)
    try:
        k.load(authfname)
    except IOError: # File doesn't exist
        conscred = query_yes_no("Do you have a CONSUMER KEY, CONSUMER SECRET pair?")
        if not conscred:
            print("Create a new application on https://dev.twitter.com/apps and try again.")
            sys.exit(0)
        else:
            k = Keychain()
            consumer_key = raw_input("Input your CONSUMER KEY: ")
            consumer_secret = raw_input("Input your CONSUMER SECRET: ")
        k.set_consumer(consumer_key, consumer_secret)
        usercred = query_yes_no("Do you have an ACCESS TOKEN, ACCESS TOKEN SECRET pair?")
        if usercred:
            access_token = raw_input("Input your ACCESS TOKEN: ")
            access_token_secret = raw_input("Input your ACCESS TOKEN SECRET: ")
        else:
            access_token, access_token_secret = authorize(consumer_key, consumer_secret)
        k.set_user(access_token, access_token_secret)

    # Save the keychain for the next time
    k.save(authfname)
    return k

def parse_arguments():
    parser = argparse.ArgumentParser(description="""Download twitter streams using the
    Streaming API""", epilog=EPILOG)
    parser.add_argument('endpoint', help='Method of the Streaming API to use',
            choices=('filter', 'sample', 'firehose', 'authorize'))
    parser.add_argument('fileprefix', help='output json to the specified file',
            action='store')
    parser.add_argument('-p', help="""add a method parameter ('name=value')""",
            metavar="PARNAME=PARVAL", action='append')
    parser.add_argument('-o', '--print', help='print every tweet', action='store_true')
    parser.add_argument('--timeout', default=30,
        help='Streaming timeout in seconds (default 30)',
        action='store', type=int, metavar="SECS")
    parser.add_argument('--delay', default=10,
        help='Sleep delay in seconds (default 10)',
        action='store', type=int, metavar="SECS")
    return parser.parse_args()


def relaunch_on_timeout(crawler, endpoint, data, timeout=30, delay=10):
    """Receive data from the specified endpoint.

    This function handles all the intermediate operations, such as building
    the URL from the endpoint and handling log messages.

    """

    # Relaunch automatically on network error / broken pipe, ...
    while True:
        try:
            LOGGER.info("Requesting stream: %s. Params: %s", endpoint, data)
            start_time = datetime.datetime.now()
            stream = crawler.request_stream(endpoint, data, timeout)
            for count, tweet in enumerate(stream):
                if not count % 1000:
                    now = datetime.datetime.now()
                    delta = now - start_time
                    rate = float(count) / delta.total_seconds()
                    # Change this to logger TODO
                    print("Total tweets", count, "\tRate", rate)
                    start_time = datetime.datetime.now()
                #print(tweet) TODO (control)
        # Unless CTRL+C or exit
        except (KeyboardInterrupt, SystemExit):
            # TODO clean outfile?
            LOGGER.info("Shutting down (manual shutdown).")
            logging.shutdown()
            sys.exit(0)
        # Handle network timeout
        except requests.exceptions.Timeout:
            # TODO introduce exp backoff
            LOGGER.info("Request timed out (timeout=%s). Waiting and retrying (delay=%s).", timeout, delay)
            time.sleep(delay)


def main():
    setup_logging()
    args = parse_arguments()
    keychain = make_keychain()

    data = {}
    if args.p:
        data = dict([i.split('=') for i in args.p])
    if args.endpoint == 'filter':
        assert set(data).intersection(('track', 'locations', 'follow'))

    filename = args.fileprefix
    consumer_key, consumer_secret = keychain.get_consumer()
    access_token, access_token_secret = keychain.get_user()
    auth = OAuth1(consumer_key, consumer_secret, access_token,
            access_token_secret, signature_type='auth_header')
    api = TwitterStreamCrawler(filename, auth)
    #api.receive(args.endpoint, data, timeout=args.timeout, delay=args.delay)
    relaunch_on_timeout(api, args.endpoint, data, timeout=args.timeout, delay=args.delay)

if __name__ == "__main__":
    sys.exit(main())
