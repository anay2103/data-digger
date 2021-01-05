#!/usr/bin/env python
# coding: utf-8

# In[ ]:


'''This module contains Scrapy spider settings.
Environment variables are loaded from the parent directory.
'''

import os
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
from scrapy.utils.log import configure_logging

#load environment variables
load_dotenv(find_dotenv('env.env'))

configure_logging(install_root_handler=False)

BOT_NAME = 'stack'

SPIDER_MODULES = ['data_digger.stack.spiders']
NEWSPIDER_MODULE = 'data_digger.stack.spiders'
ITEM_PIPELINES = {'data_digger.stack.pipelines.StackPipeline':300}

#Environment variables loaded from a .env file
MONGO_URI = os.getenv('MONGO_URI')
MONGODB_DB = os.getenv('MONGO_DB')
MONGODB_COLLECTION = os.getenv('MONGODB_COLLECTION')

TWISTED_REACTOR = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'

DEFAULT_REQUEST_HEADERS= {
         "Accept":"*/*",
         "Accept-Encoding" :"gzip, deflate, br",
         "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
         "Cache-Control" : "max-age=0",
         "Connection": "keep-alive",
         "Host": "seekingalpha.com",
        }

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:82.0) Gecko/20100101 Firefox/82.0'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True
# Disable logs as logging.basicConfig() is called from Mongo_module
LOG_ENABLED = False
# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 1

# Configure a delay for requests for the same website (default: 0)
DOWNLOAD_DELAY = 2
CONCURRENT_REQUESTS_PER_DOMAIN = 2

# Disable cookies (enabled by default)
COOKIES_ENABLED = False
# Max number of items to be scraped before spider gets closed  
CLOSESPIDER_ITEMCOUNT= 2000

#Disable Scrapy built-in useragent middleware and
#enable random User Agent middlware to avoid 403 errors:
# https://pypi.org/project/scrapy-fake-useragent/
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
    'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
    'scrapy_fake_useragent.middleware.RetryUserAgentMiddleware': 401,
    'data_digger.stack.middlewares.StackDownloaderMiddleware': 543,
}
#Setting recommended by scrapy-fake-useragent
FAKEUSERAGENT_PROVIDERS = [
    'scrapy_fake_useragent.providers.FakeUserAgentProvider',  # this is the first provider we'll try
    'scrapy_fake_useragent.providers.FakerProvider',  # if FakeUserAgentProvider fails, we'll use faker to generate a user-agent string for us
    'scrapy_fake_useragent.providers.FixedUserAgentProvider',  # fall back to USER_AGENT value
]

# Enable and configure the AutoThrottle extension (disabled by default)
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 5

