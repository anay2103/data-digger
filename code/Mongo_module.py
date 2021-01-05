#!/usr/bin/env python
# coding: utf-8

# In[1]:


'''this module is the central part of the project 
equipped with the following functionality:
    * running a webscraper 
    * building a queue of articles stored with the Mongo database,
      searching for stock tickers in each article,
      requesting Alpaca API for historical stock quotes,
      updating database documents with the received stock quotes.
    * labelling database documents with '1' or '0' flag
      depending on whether the article publication was followed
      by abnormal returns of stocks referred to in the given article
      
environment variables are loaded from the file located in the module directory.
'''

import os
import sys
from pathlib import Path

import asyncio 
import pymongo
import dns
import logging
import time 

from datetime import datetime
from functools import partial 
from dotenv import load_dotenv, find_dotenv
from data_digger.stack.misc_functions import * 
import data_digger.Alpaca as alp

# load environment variables
load_dotenv(find_dotenv('env.env'))    

logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        level=logging.INFO,
        datefmt="%H:%M:%S",
        )
logger = logging.getLogger("debugger")

class MongoHandler:
        '''A class encapsulating most of package functionality.
        
        Attributes:
            collection: a string name of Mongo collection 
                        where the scraped data is stored
            client: a MongoClient instance
            db: a pymongo.database instance
            field: a string name of a database field 
                    with historical stock quotes
        '''
        
        def __init__(self, field):
            '''inits MongoHandler class
            
            Args:
                field: a string variable assigned by the user.
                It is assumed that the same field name is used
                each time historical quotes are added to the collection.
            '''
            
            db = os.getenv('MONGO_DB')
            uri = os.getenv('MONGO_URI')
            self.collection = os.getenv('MONGODB_COLLECTION')
            self.client = pymongo.MongoClient(uri)
            self.db = self.client[db]
            self.field = field
        
        def crawler(self):
            '''runs a built-in Scrapy scraper '''
            try:
                from data_digger.stack.spiders.HAR_crawler import HARSpider
                from scrapy.crawler import CrawlerProcess
                from scrapy.utils.project import get_project_settings
                from scrapy.utils.log import configure_logging
            except ImportError:
                logger.exception("Scraper not found!")
                sys.exit(1)
            else:
                 settings_file_path = "data_digger.stack.settings"
                 os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
                 process = CrawlerProcess(get_project_settings())
                 process.crawl(HARSpider)
                 process.start()
                    
                    
        def find_item(self, q,full, empty, maxsize=None):
            '''makes an iterable from database documents
               and puts them into a limited size queue.
               
               Args:
                    q: a queue-like list to append database documents
                    full, empty: threading condition variables locking
                    producer and consumer threads while items are processed
                    and releasing when the queue is empty
                    maxsize: an integer defining max queue length
            '''
            
            if maxsize is None:
                maxsize=500
            cursor = self.db[self.collection].find({self.field: {"$exists": False }})
            for i in cursor:
                with full:
                    while len(q)>=maxsize:
                        print(f"Queue is full and size = {len(q)}")
                        empty.notifyAll()
                        full.wait()
                    q.append(i)
                    empty.notifyAll()
            [q.append(None) for i in range(10)]
            print ('all items added')
        
        def updater(self, q, full, empty):
            '''loops through consumed news articles
            and searches for ticker symbols in them.
            
            instantiates Alpaca class and makes API requests 
            for stock quotes around a given article publication date.
            Then dumps them to database with an asyncio callback.
            
            Args:
                same as for self.find_item
      
            '''
            #a new loop for each thread the task is distributed to
            newloop = asyncio.new_event_loop()
            asyncio.set_event_loop(newloop)
            inst = alp.Alpaca ()
            while True:
                with empty:
                    while len(q) ==0:
                         print(f"Queue is drained, recharging...{len(q)}")
                         full.notify()
                         empty.wait()
                    item = q.pop() 
                    if (item == None):
                        self.loop_shutdown(newloop)
                        break
                    time = item['time']
                    tickers = ticker_extraction(item ['article'])
                    res =[]
                    for i in tickers:
                        task = asyncio.ensure_future(inst.make_request(time, ticker=i))
                        task.add_done_callback(partial (self.db_inserter, item=item))
                        res.append(task)
                    try:
                        fin = newloop.run_until_complete(asyncio.gather(*res))
                    except Exception as e:
                         logger.exception ("An error occurred at updater func: %s", e)  
                    finally:
                        asyncio.sleep(1)
                        full.notify()
                       
                    
        def db_inserter(self, res, item):
            '''a callback function adding new field 
               with stock quotes to the database.
               
               Args: 
                   res: Alpaca.make_request coroutine wrapped into an asyncio task
                   item: a database document to be updated
            '''
            
            try:
                data = res.result()
                if data== None:
                    logger.info ("A None has arrived:(...")
                else:
                    upd = self.db[self.collection].update_one({'article': item ['article']}, {'$push': 
                                                                    {self.field: data}})
                  #  logger.info (upd.modified_count)
            except  Exception as e:
                logger.exception ("An error occurred at db_inserter: %s", e)  
                    
        def loop_shutdown(self, loop):
            '''graceful shutdown of asyncio pending tasks.
            
            Args:
                loop: a running event loop to be stopped
            '''
            
            pending= [t for t in asyncio.Task.all_tasks()]
            for p in pending:
                p.cancel()  
            try:
                loop.run_until_complete(asyncio.gather(*pending))
            except Exception as e:
                logger.exception ("An error occurred at shutdown: %s", e) 
            loop.stop()
            
        
        def labelling(self):
            '''labels database articles with '1' if the stock mentioned in a given article
            gained abnornal returns greater then 2 per cents in absolute terms 
            right after the publication or '0' otherwise.
            
            If more than one stock is mentioned in the article,
            an output document for each stock is created using "$unwind" mongo command.
            
            Outputs the results to another collection to avoid confusion.
            '''
            
            var = f"{self.field}"
            subvar = var+"."+alp.Alpaca.returns
            labels =self.db[self.collection].aggregate([
                           {"$unwind": "$"+var},
                           {"$match" : {
                               var: {
                                    "$exists": True },
                               var: {
                                    "$ne": None}
                           }}, 
                           {"$addFields":{
                               "label":{
                                    "$switch":{
                                        "branches":[
                                            {"case": {"$or":[{"$lt":[subvar, -2]},
                                                              {"$gt":[subvar, 2]}]},
                                             "then":"1"},
                    
                                            {"case": { "$and":[{"$lt":[subvar,2]}, 
                                                               {"$gt": [subvar,-2]}]},
                                             "then":"0"}
                                        ],
                                         "default": "None"
                                    }
                                }
                           }},
                           {"$project":{"_id":0}},
                           {"$out": "another_collection"}
                    ])  
            logger.info ('Data is labelled now!')



