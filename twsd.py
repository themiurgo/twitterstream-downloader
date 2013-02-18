#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import datetime
import urllib2
import sys

import requests
from requests_oauthlib import OAuth1
import json

import db

DESCRIPTION = """Download tweets in realtime using the Twitter Streaming API.

"""

EPILOG = """Requires Python Requests and Python Requests Oauthlib."""

class TwitterStreamCrawler(object):
    def __init__(self, base_filename, user_key, user_secret, app_key,
            app_secret):
        self._db_instance = db.FileAppendDb(base_filename)
        self._auth = OAuth1(app_key, app_secret, user_key, user_secret, 
                      signature_type='auth_header')

    def receive(self, endpoint, data, print_tweets=False):
        url = 'https://stream.twitter.com/1.1/statuses/{0}.json'.format(endpoint)
        db_instance = self._db_instance
        count = 0
        start_time = datetime.datetime.now()
        if not print_tweets:
            do = lambda x: None
        else:
            def do(tweet):
                print(json.loads(tweet)['text'])
        #coords = "-180,-90,180,90"
        #for i in api.statuses.sample():
        #kw = {'track': ','.join(keywords)}
        r = requests.post(url, data=data, auth=self._auth, stream=True)
        for line in r.iter_lines():
            #do(line)
        #for i in api.statuses.filter(locations=coords):
           # except urllib2.URLError as e:
           #     print str(e)
           #     if isinstance(e.reason, socket.timeout):
           #         with open("error.log", "a") as f:
           #             f.write("Error ")
           #             f.write(str(datetime.datetime.now()))
           #             f.write("\n")
           #         continue
            #db_instance.save(line)
            try:
                pass
                #print i['text']
            except:
                raise
                print("ERROR")
                pass
            count += 1
            now = datetime.datetime.now()
            delta = now - start_time
            rate = float(count) / delta.total_seconds()
            if not count % 100:
                print("Total tweets", count, "\tRate", rate)

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
    args = parser.parse_args()

    endpoint = args.endpoint
    data = {}
    if args.p:
        data = dict([i.split('=') for i in args.p])
    if endpoint == 'filter':
        assert set(data).intersection(('track', 'location', 'follow'))

    filename = args.file
    api = TwitterStreamCrawler(filename, args.uk, args.us, args.ck, args.cs) 
    api.receive(endpoint, data, print_tweets=True)
