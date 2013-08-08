twitterstream-downloader
------------------------

Twitterstream-downloader (in the following `twsd`) is a Python script that allows to download streams of tweets using the Twitter Streaming API.

Dumping all the tweets related to Arcade Fire and Radiohead is as simple as:

    twsd filter -p track='Arcade Fire,Radiohead' -f music_news
    
This will download the Streaming messages to daily files
`music_news_20130801.json`, `music_news_20130802.json`, ...

Installation
------------

	pip install git+git://github.com/themiurgo/twitterstream-downloader


Usage
-----

    twsd ENDPOINT FILE_PREFIX [-p PARNAME=PARVAL]

* `ENDPOINT` can be either `sample` or `filter` or `firehose` (read more on [Twitter Streaming API docs (Public Streams)](https://dev.twitter.com/docs/streaming-apis/streams/public). The fictious `authorize` endpoint is used to authorize a new client.
* `FILE_PREFIX` is the path+prefix of the files related the stream.

Features
--------
* Download streams from Twitter Streaming API
* Rotate files every day
* Automated OAuth authentication and key management.
* Log error messages

Examples
--------
Download some data from the Twitter Sample endpoint to files `~/tweets/sample_YYYYMMDD.json`. (This will also create a logfile `~/tweets/sampledump.log` containing events related to the stream (crawling start, stop, limit messages).
    
    twsd sample ~/tweets/sampledump
    
Download all the geolocated tweets in UK and Ireland to files `~/project_ukir/tweets_YYYYMMDD.json` (and create a log file, as specified above).

	twsd filter ~/project_ukir/tweets -p locations="-180,-90,180,90"

Configuration
-------------

Twsd will attempt to locate a configuration file `~/.twsd` with authentication credentials. When this fails, it will ask OAuth credentials.