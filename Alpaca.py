#!/usr/bin/env python
# coding: utf-8

# In[13]:


'''module designed to fetch historical stock quotes from Alpaca Stock Trading API.

pandas_market_calendars library should be imported to detect market holidays.
environment variables are loaded from the file located in the module directory.
'''

import json
import aiohttp
import asyncio
from datetime import datetime, timedelta, date
import pandas_market_calendars as mcal
from functools import partial, wraps
import logging
import os
import numpy as np 
import sys 
import operator
from dotenv import load_dotenv
from pathlib import Path


env_path = Path ('stack')/'.env'
load_dotenv(dotenv_path=env_path)

logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        level=logging.DEBUG,
        datefmt="%H:%M:%S",
    )
logger = logging.getLogger("debugger")


class Alpaca:
    '''A class designed for making API requests and processing the results
    
    Atributes:
        key, s_key: API keys stored in a .env file
        open_hour, open_minute, close_hour, close_minute: integers used to define NYSE regular trading hours
        nyse, early: calendar values returned by pandas_market_calendars methods 
        returns: a string variable used as a class output name
        date: a datetime object containing date and time of article publication
        weekday: an integer representation of weekday when an article was published  
        weekend: a boolean indicating ehether a weekday is weekend or not
        loop: asyncio loop running http requests 
    '''
    
    key = os.getenv ('APCA-API-KEY-ID')
    s_key = os.getenv ('APCA-API-SECRET-KEY')
    
    open_hour = 9
    open_minute = 30
    close_hour = 15
    close_minute = 00
    nyse = mcal.get_calendar('NYSE')
    early = nyse.holidays()
    returns= 'abnormal returns'
    
    def __init__(self): 
        '''Inits Alpaca class'''
        
        self.date = None 
        self.weekday = None
        self.weekend = None        
        self.loop = asyncio.get_event_loop()
          
    def _if_holiday(calendar):
        '''a decorator adjusting a stock quotes request for holidays and weekends.
        Necessary since Alpaca API does not make this adjustment internally
        '''

        def holiday_decorator(func):
            @wraps(func)
            def holiday_wrapper (*args, **kwargs):
                '''a wrapper detecting if a start date and an end date
                returned by wrapped self.date_formatter are holidays or weekends.
                If True, replaces the start date with the preceding workday 
                and the end date with the next workday.
                '''
                
                def date_shifter(date, holid= None, operator= None):
                    while any([date.date() in holid, date.isoweekday()>5]):
                        date = operator(date, timedelta(days=1))
                    return date
                holid = [i.astype(datetime) for i in calendar.holidays]   
                start, end = func (*args,**kwargs)
                start_h, end_h = [any([x.date() in holid, x.isoweekday()>5]) for x in (start, end)]
                shifter = partial(date_shifter, holid = holid)
                if start_h:
                    start= shifter (start, operator = operator.sub)
                if end_h:
                    end = shifter(end, operator = operator.add)
                return [date.isoformat() + '-04:00' for date in (start, end)]
            return holiday_wrapper
        return holiday_decorator
        
        
    @_if_holiday(calendar = early)
    def date_formatter(self, date_close):
        '''defines the start date and the end date for a stock quotes request
        
        if an article was published before 3 p.m., 'start' variable is 
        assigned with a day prior to the publication date and 
        'end' variable is assigned with the publication date itself.
        otherwise, 'start' means the publication date 
        and 'end' means a day after the publication. 
        '''
        
        if self.date>date_close:
            start = self.date 
            end = (self.date+timedelta(days=1)) 
        else:
            start = (self.date- timedelta(days=1)) 
            end = self.date 
        return start, end
                
    def url_formatter(self, date_close):
        '''raises a ValueError if an article was published in the weekend or after the Friday close.
        Otherwise, returns a tuple of strings to be used as URL string parameters 
        '''
        
        if any([self.weekend, (self.date.isoweekday()==5 and self.date>date_close)]):
            raise ValueError ("sorry, weekend has come!: " + str(self.date))
        else:
            return 'until', 'start', 'end'       
        
        
    async def find_quote (self, session, url, ticker): 
            '''requests an API endpoint for historical stock quotes.
            
            Args:
                session: aiohttp session
                url: URL string to be requested
                ticker: stock ticker name of a string type, whose quotes are requested
                
            Returns:
                json response containing list of dicts with daily bar values.
                For example:
                [{'t': 1602129600, 'o': 342.85, 'h': 343.85, 'l': 341.86, 'c': 343.73, 'v': 35858727},
                {'t': 1602216000, 'o': 345.56, 'h': 347.35, 'l': 344.89, 'c': 346.84, 'v': 45969566}]
            
            Raises:
                ValueError: an error occured if no historical data 
                was found for a given stock ticker. For example, 
                when a stock began being publicly traded after the publication date,
                or a stock is OTC traded  
            '''
            
            tasks = {asyncio.ensure_future((session.get(url, 
                                    headers = {"APCA-API-KEY-ID": self.key, "APCA-API-SECRET-KEY": self.s_key}))):
                     url}
            pending= set (tasks.keys())
            count = 0
            while pending and count<=5:
                count+=1
                done, pending = await asyncio.wait (pending, return_when=asyncio.FIRST_EXCEPTION)
                for d in done:
                    if d.exception():
                        logger.info(f'An exception {d.exception()}, retrying...')
                        pending.add(d)
                        await asyncio.sleep(2)
                    else:
                        try:
                            res1 = await d.result().json()
                        except Exception as e: 
                            logger.info("An error occurred at find_quote %s", e)  
                        else:
                            if res1[ticker] ==[]:
                                raise ValueError('invalid ticker!' +ticker)
                            else:
                                return res1
                         
    
    async def make_request(self, date, ticker): 
        '''an entry point method for making API requests
           intended to be an interface for other modules.
           
           Historical data received with an API response is processed further
           in the executor and only then returned by the method. 
           Logs the exception if two time series submitted to the executor
           are of different length. This happens if the number of 
           daily bars received for a given ticker is less than the number 
           of daily bars available for SPY, a market benchmark. 
           
           Args:
               date: a datetime object containing date and time of article publication
               ticker: stock ticker name of a string type extracted from an article
           
           Returns:
               a list of dicts containing the results of data processing in the executor 
        
           Raises:
               ValueError: an error occured determining the start and end dates of a time window
           '''
        self.date = date
        self.weekday = self.date.isoweekday()
        self.weekend = self.weekday>5 # if a publication was made in the weekend. 
        date_open = self.date.replace(hour = self.open_hour, minute = self.open_minute)
        date_close =self.date.replace(hour = self.close_hour, minute = self.close_minute)
        
        async with aiohttp.ClientSession() as session:
            param1, param2, param3 = self.url_formatter(date_close)  
            
            try:
                start, end = await self.loop.run_in_executor(None, partial (self.date_formatter, date_close))
            except Exception as e:
                raise ValueError ('invalid date format'+ str(self.date))
            else:
                urls = [f'https://data.alpaca.markets/v1/bars/day?symbols={ticker},SPY&limit=30&{param1}={start}',
                    f'https://data.alpaca.markets/v1/bars/day?symbols={ticker},SPY&{param2}={start}&{param3}={end}']
            
                task1, task2 = [asyncio.ensure_future(
                            self.find_quote (session, url, ticker)) for url in urls]
                try:
                    before, after = await asyncio.gather (task1, task2)
                except Exception as e:
                    logger.info("An error occurred while requesting quotes %s", e)  
                else:
              
                    try: 
                        ts = await self.loop.run_in_executor(None, partial(self.OLS_method, before, after, ticker)) # now wait for OLS regression
                    except Exception as e:
                        logger.exception ("An error occurred while processing results: %s", e)  
                    else:
                        return ts
                    
   
    def OLS_method(self, before, after, ticker):
        ''' calculates abnormal stock returns according to the event study methodology:
        
            https://en.wikipedia.org/wiki/Event_study
            https://www.jstor.org/stable/2729691?seq=1
            
            an abnormal return is defined as the error term 
            of the linear regression of stock returns exhibited
            after the publication date against SPY returns.
            
         Args: 
            before: list of dicts with 30 daily bar values before the publication date
            after: list of dicts with daily bar values after the publication date
            ticker: stock ticker name of a string type
        
         Returns:
            a dict mapping string keys to the given ticker and
            calculated alpha, beta and abnormal return values. 
            'Event_window' key stores ticker returns exhibited after the publication.
        '''
        def rets (x):
            return (np.diff(x)/x[:-1])*100
             
        market_data, market_event = (([i['c'] for i in before['SPY']]),
                                     ([i['c'] for i in after['SPY']]))
                        
        item_data, item_event = (([i['c'] for i in before[ticker]]),
                                 ([i['c'] for i in after[ticker]]))
    
        X = np.stack ((rets (item_data), rets (market_data)), axis =0)
        cov = np.cov (X)
        market_var = np.var(rets (market_data))
        
        beta = np.true_divide(cov,market_var)
        alpha = np.mean(rets (item_data))- beta[0,1]*np.mean(rets (market_data))
       
        item_rets = rets(item_event)
        market_rets = rets(market_event)
        ab_rets = item_rets- alpha- beta[0,1]*market_rets
        CAR = np.sum (ab_rets) # if an array of returns has more than one element, calculates cumulative abnormal returns 
        return { 'ticker': ticker, 
                  'alpha':alpha,
                  'beta':beta[0,1], 
                   self.returns: CAR,
                  'event_window':[ 
                                {str(datetime.fromtimestamp(i['t'])):i['c']} for i in after[ticker]
                            ]}
    
                                 


# In[ ]:




