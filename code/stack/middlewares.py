from scrapy import signals
from scrapy.http import Request
from scrapy import Request, exceptions

class StackDownloaderMiddleware:
    '''a custom Scrapy downloader middleware class
        which raises DontCloseSpider exception
        when the spider is idle.
    '''
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        s =  cls()
        crawler.signals.connect(s.spider_exhausted, signal=signals.spider_idle)
        return s
    
    def spider_exhausted(self, spider):
        spider.logger.info('Spider exhausted: %s' % (spider.name))
        request= Request (spider.next_url, dont_filter= True, callback = spider.to_next_page)
        spider.crawler.engine.crawl (request, spider)
        raise exceptions.DontCloseSpider
        

