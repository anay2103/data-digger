'''this module contains miscellaneous functions:

    * ticker_extraction - searches for stock tickers using regex
    * tokenize- tokenizes text into sentences
    * collect_bins - bins sentence length values
    * deleter - deletes articles composed from shortest sentences 
'''

import re 
import numpy as np
from itertools import chain,product,starmap
from nltk import sent_tokenize

def ticker_extraction (text):
    '''looks up for NYSE and NASDAQ stock tickers 
       in the first two sentences of a given article. 
       Standard journalistic approach is to convey the gist in the beginning.
       
       Args: 
           text: an article to be looked up (a string variable)
    
       Returns:
           a list of finded stock tickers
        
       Raises:
           IndexError: an error occurred at a single-sentenced article, if any
    '''
    
    dot = r'(?<![A-Z])\.\s*(?=[A-Z“]\w+)'
    abb1 = r'(?:(?<=NASDAQ:)|(?<=NYSE:)|(?<=NYSE\w{3}:)|(?<=NYSE\w{4}:))(?:[A-Z]{1,}|[A-Z.&]{2,})'
    abb2 = r'(?:[A-Z]{1,}|[A-Z.&]{2,})(?=[\s:,]*[-+]*?(?:\d+(?:\.\d*)?|\.\d+)%*\s*\))'
    sentences = re.split (dot, text)
    try:
        it = sentences[0:2]
    except IndexError:
        it = sentences[0]
    finally:
        abbv = (product((abb1, abb2), it))
        return list( chain(*starmap (re.findall, abbv))) 

def tokenize(text):
    '''adds a whitespace after dots finalizing sentences, and then
       tokenizes text into sentences using brilliant nltk library.
        
    Args:
        text: a string of whole corpus of sentences stored in the database
    
    Returns:
        an iterator of sentences (tokens) 
    '''
    
    dot = r'\.(?=[A-Z])'
    subst = re.sub(dot, r'. ', text)
    sents = sent_tokenize(subst)
    for s in sents:
        yield s
        
            
def collect_bins (db, collection):
    '''aggregates all database articles into one text 
       and feeds it into the sentence tokenizer.
       
       Finds min and max sentence length values.
       Creates an array of bins within these limits.
       
       Args:
           db: a pymongo.database in which the data is stored
           collection: a string name of Mongo collection 
           in which articles are stored.
           
       Returns:
           a numpy array of bins
    '''
    
    view = db[collection].aggregate( [  {"$group":{
                                        "_id": None, 
                                        "mergedDocs": {"$addToSet": "$article"}
                                    }},
                                    {"$project":{
                                            "final":{
                                                "$reduce": {
                                                    "input": "$mergedDocs",
                                                    "initialValue": "",
                                                    "in": { "$concat" : ["$$value", "$$this"]}
                                                                }
                                                            }
                                                     }}
                                                  ])
                                   
    doc = view.next()['final']
    total_sents = tokenize(doc)
    x = np.array([(len(s)) for s in total_sents])
    min_edge, max_edge = np.min(x), np.max(x)
    bins = np.linspace(min_edge, max_edge, 20)
    return bins
              

def deleter(q,bins, db, col):
    '''iterates through s queue and deletes articles 
       every sentence in which belongs to the leftmost bin.
       
       Args:
            q: a queue object with database documents already put in
            bins: an array of bins to index sentences with
            db: a pymongo.database in which the data is stored
            collection: a string name of Mongo collection,
            in which the data is stored. 
    '''
    
    while True:
        item = q.get()
        if (item == None):
            print('item is none! ')
            break        
        toks = tokenize(item['article'])
        digit = [np.digitize(len (tok), bins) for tok in toks]
        short = all(d==1 for d in digit)
        if short is True:
            try:
                db[col].delete_one( { "_id":item['_id']})
            except Exception as e:
                logger.exception ("An error occurred: %s", getattr(e, "__dict__", {}))  
        q.task_done()


# In[ ]:




