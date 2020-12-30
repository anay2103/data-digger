#!/usr/bin/env python
# coding: utf-8

# In[ ]:


'''This is a standard Scrapy spider scraping latest stock market news
 from https://seekingalpha.com/
'''

import json
import re
import logging
import bs4
from datetime import datetime, date, timedelta, time 
import pytz # a library for timezone calculations

from scrapy import Spider
from scrapy.selector import Selector
from scrapy import Request, signals

from scrapy import exceptions
from scrapy import settings
from twisted.internet import defer

from twisted.internet.asyncioreactor  import AsyncioSelectorReactor as asyncreactor
from data_digger.stack.items import StackItem


class HARSpider(Spider):
    '''a Spider class
      
       Attributes:
          name: a string spider name 
          allowed_domains: a string domain name allowed to crawl
          handle_httpstatus_list: a list of http error codes the spider 
          is enabled to handle
          tz: a pytz.timezone object aware of US Eastern local time
          year: a datetime object method storing info about the current year  
          timeout: an integer number of seconds to wait  before retrying
          unsuccessful http request
          next_url: a URL string for the spider to crawl next
      '''
    
    name = "HAR_crawler"
    allowed_domains = 'seekingalpha.com'
    handle_httpstatus_list = [304,403]
    tz = pytz.timezone('US/Eastern')
    year = datetime.today().year
    
    def __init__(self, *args, **kwargs):
        '''inits the spider'''
        self.timeout = float(kwargs.pop('timeout', '60'))
        self.next_url = None
        self.adjust_logging ()
        
    def adjust_logging(self):
        formatter = logging.Formatter(
                    '%(asctime)s [%(name)s] [%(levelname)s] %(message)s')
        root_logger = logging.getLogger()
        handler = root_logger.handlers[1]
        handler.setFormatter(formatter)

    def start_requests(self):
        '''returns an iterable with the first request to crawl'''
        return [Request("https://seekingalpha.com/market-news/all", dont_filter=True, 
                        callback=self.parse_item)]
    

    
    def to_next_page(self, spider):
        '''yields a new Request object for the spider to crawl next'''
        request = Request(self.next_url,  dont_filter=True, 
                                callback=self. parse_item)
        self.log ("next page coming...")
        yield request
          
        
    def repeat (self, response):
         '''repeats a failed Request until the number of retries is exhausted.
            returns a Deferred object.
         '''
         df= defer.Deferred()
         rep_url = response.url
         c = response.request.meta['count']
         rep_req = Request (rep_url, dont_filter=True,
                           callback = self.parse_item, 
                           meta={
                             'count':c},
                          )
         if c<=10:
             asyncreactor.callLater(asyncreactor(),self.timeout, df.callback, rep_req)
             return df
         else:
            self.log('Attemps exausted:' + rep_url + "  give up retrying")
            
            
    def parse_item(self,response):
            '''parses Response received from the URL.
        
            retries and returns None if Response status differs from 200. 
            An inner function returns a next page url string.
            '''
        
            if response.status!=200:
                try:
                    c = response.request.meta['count']+1
                except KeyError:
                     c= 0
                self.log ("403 Error, retrying... "  + response.url)
                yield Request (response.url, dont_filter=True,
                           callback = self.repeat, 
                           meta={
                             'count':c })
                return
        
            
            def next_page_parser(soup):
                page_url = soup.find(class_= 'list-inline').find("a", text ='Next Page').get('href')
                next_url = response.urljoin(page_url)
                return next_url
        
            soup = bs4.BeautifulSoup(response.body, "html.parser")
            self.next_url = next_page_parser(soup)
            
            # iterates through the html class of interest and assigns variables 
            # to Scrapy items fields 
            for el in soup.find_all(class_ = 'bullets item-summary hidden'):
                 item = StackItem()
                 item['article']= el.get_text()
                 time_tag = el.parent.find (class_='item-date')
                 time_search= re.search (r'(?<=>).+?(?=<)', str (time_tag))
                 try:
                     time = datetime.strptime (time_search.group (0), '%a, %b. %d, %I:%M %p').replace(year = self.year)
                 except Exception as e:
                    if 'Today' in time_search.group(0):
                        date = datetime.now(self.tz)
                    elif 'Yesterday' in time_search.group(0):
                        date = datetime.now(self.tz) - timedelta(days=1)   
                    word, ampm = [*time_search.group(0).split(', ')]
                    time_conv = datetime.strptime(ampm, '%I:%M %p').time()
                    time = datetime.combine (date, time_conv)   
                 item['time'] = time 
                 title = el.parent.find (class_='title')
                 item['title'] = title.get_text()
                 url = title.a.get('href')
                 full_url = response.urljoin(url)
                 item['url'] = full_url
                 yield item

