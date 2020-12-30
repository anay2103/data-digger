#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pymongo
import dns
import logging
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from scrapy import settings
from functools import partial
from datetime import datetime
#imports the function searching for stock tickers in a given text 
from data_digger.stack.misc_functions import ticker_extraction



class StackPipeline(object):
    '''a custom Scrapy Pipeline class writing items to Mongo DB.
    
       Attributes:
           mongo_uri = a Mongo URI string got from Scrapy.crawler.settings
           mongo_collection= a Mongo collection string name in which items should be written
           mongo_db = a Mongo database name in which items should be written
           db = a pymongo.database instance
           client = a MongoClient instance
           db = a pymongo.database instance
           collection = a pymongo.collection instance
    '''
        
    def __init__(self, mongo_uri, mongo_db, mongo_collection):
        '''Inits StackPipeline '''
        self.mongo_uri = mongo_uri
        self.mongo_collection = mongo_collection
        self.mongo_db = mongo_db
    
        
    @classmethod
    def from_crawler(cls, crawler):
        '''passes settings parameters to the __init__ method.
        
           Args:
               crawler: a Scrapy.crawler instance
        '''
        return cls(
            mongo_uri = crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGODB_DB'),
            mongo_collection=crawler.settings.get('MONGODB_COLLECTION')
        )
        
    def open_spider(self, spider):
        '''instantiates MongoClient and attaches a custom
           class instance to the Scrapy spider.
            
           Args:
                spider: a Scrapy.spider instance
        '''
        
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db] 
        self.collection = self.db[self.mongo_collection]
        
    def close_spider(self, spider):
        '''closes MongoClient connection'''
        self.client.close()
        
    async def process_item(self, item, spider):
        '''drops articles with no mention of stock tickers.
           Places all others to the Mongo collection.
        '''
           
        tickers = ticker_extraction (item['article'])
        if tickers == []:
            raise DropItem (f"item with no news:{item}")
  
        self.collection.insert_one(ItemAdapter(item).asdict())
        return item
    

