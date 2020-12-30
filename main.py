#!/usr/bin/env python
# coding: utf-8

# In[ ]:


'''This script parses command-line arguments 
and calls an appropriate class method.

Tasks solved with iteration through a database collection 
are allocated among multiple threads.
'''

import sys
import argparse
import time
from data_digger import Mongo_module


parser = argparse.ArgumentParser()
parser.add_argument ("--articles", help = "scrapes recent stock news articles from https://seekingalpha.com/",
                      action="store_true")
parser.add_argument("--quotes", help = "gets stock quotes from Alpaca API and inserts them into the database",
                    action="store_true")
parser.add_argument("--sweeper", help= "deletes useless articles containing sentences of minimal length."
                                        "Sentences are tokenized and split into bins depending on their length."
                                        "Articles in which each sentence belongs to first (minimal) bin are deleted",
                   action="store_true")
parser.add_argument("--label", help = ("labels the documents in the database with '1' "
                                       "if absolute value of abnormal returns is greater than 2%% or '0' otherwise"),
                   action = "store_true")

args = parser.parse_args()
if len(sys.argv)==1:
    parser.print_help(sys.stderr)
    sys.exit(1)

#instatiates Mongo_module class
handler = Mongo_module.MongoHandler ("EVENT_STUDY")

if args.articles:
    handler.crawler()

elif args.quotes:    
    from threading import Thread, Lock, Condition
    start_time = time.time()
    q = []
    lock = Lock()
    full_= Condition(lock)
    empty_ = Condition(lock)
    # a single producer thread retrieving documents from a database
    m = Thread(target = handler.find_item, args =(q, full_, empty_))
    m.start()
    threads = []
    # multiple consumer threads calling the API and dumping results to a databse 
    for l in range (10):
        t = Thread(target = handler.updater, args= (q, full_, empty_))
        t.start()
        threads.append(t)
    m.join()

    for t in threads:
        t.join()

    print("--- %s seconds ---" % (time.time() - start_time))
    
elif args.label:
    handler.labelling()

elif args.sweeper:
    from threading import Thread
    from queue import Queue
    # conditional import of functions which bin sentences and delete short articles
    from data_digger.stack.misc_functions import collect_bins, deleter
    q = Queue()
    cursor = handler.db[handler.collection].find()
    bins = collect_bins(handler.db, handler.collection)
    print ("bins collected. Enqueuing..")
    for l in range(10):
        w = Thread (target = deleter, args = (q, bins, handler.db, handler.collection))
        w.start()
    for i in cursor:
        q.put(i)
    print ("all items put into queue")
   
    for c in range(10):
        q.put(None)
        q.task_done()
    q.join()
    print ("Deleting threads terminated")

