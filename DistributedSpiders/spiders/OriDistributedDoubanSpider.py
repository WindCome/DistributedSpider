import json
import queue
import re

import pika
from lxml import etree

import scrapy

from DistributedSpiders.items import DistributedSpidersItem


class DistributedDoubanSpider(scrapy.Spider):
    name = 'Dist_Douban'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.rabbitMQPipeline = None
        self.queue = queue.LifoQueue()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='121.36.82.230', port=5672))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='douban')
        self.channel.basic_consume(queue='douban', on_message_callback=self.callback, auto_ack=True)

    def async_consuming(self):
        self.channel.start_consuming()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(DistributedDoubanSpider, cls).from_crawler(crawler, *args, **kwargs)
        from scrapy import signals
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def _get_from_queue(self):
        try:
            item = self.queue.get()
        except queue.Empty:
            return None
        if item['mark'] == 'crawl':
            yield scrapy.Request(url=item['data'], callback=self.find_info_page)
        elif item['mark'] == 'info':
            yield scrapy.Request(url=item['data'], callback=self.parse)

    def spider_idle(self, spider):
        for request in self._get_from_queue():
            print('idle')
            self.crawler.engine.crawl(request, self)
        from scrapy.exceptions import DontCloseSpider
        raise DontCloseSpider()

    def start_requests(self):
        url = "https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=&start={}"
        for i in range(0, 3):
            current_url = url.format(i * 20)
            item = DistributedSpidersItem()
            item['mark'] = 'crawl'
            item['data'] = current_url
            self.rabbitMQPipeline.process_item(item, self)
        import threading
        t = threading.Thread(target=self.async_consuming)
        t.start()
        for request in self._get_from_queue():
            yield request

    def callback(self, ch, method, properties, body):
        data = str(body, encoding='utf-8')
        tmp = json.loads(json.loads(data))
        item = DistributedSpidersItem()
        item['mark'] = tmp['mark']
        item['data'] = tmp['data']
        # print("[x] Received %r" % body)
        self.queue.put(item)

    def find_info_page(self, response):
        import json
        info = json.loads(response.body.decode(response.encoding))
        data = info["data"]
        for x in data:
            item = DistributedSpidersItem()
            item['mark'] = 'info'
            item['data'] = x["url"]
            yield item

    def parse(self, response):
        doc = etree.HTML(response.body.decode(response.encoding))
        star = doc.xpath('//strong[@class="ll rating_num"]')[0].text
        year = doc.xpath('//span[@class="year"]')[0].text
        search = re.search('(\d{4})', year)
        year = search.group(1)
        movie_name = doc.xpath('//span[@class="year"]/preceding-sibling::span[1]')[0].text
        item = DistributedSpidersItem()
        item['mark'] = 'data'
        item['data'] = json.dumps({"year": year, "name": movie_name, "star": star}, ensure_ascii=False)
        yield item

