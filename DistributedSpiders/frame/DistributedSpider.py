import json
import queue
import threading

import scrapy
from scrapy.conf import settings

from DistributedSpiders.frame.Middleware import RabbitMQManager
from DistributedSpiders.items import DistributedSpidersItem


class DistributedSpider(scrapy.Spider):

    def __init__(self, task_id="", **kwargs):
        super().__init__(**kwargs)
        self.duplicateFilterPipeline = None
        self.rabbitMQPipeline = None
        self.task_id = task_id
        self.channel_name = task_id+self.name
        self.queue = queue.LifoQueue()
        self.__callback_map = {}
        self.__mq_timeout = int(settings.get("DOWNLOAD_DELAY"))*10
        self.__consumer_thread = None
        self.__commit_map = {}

    def __async_consuming(self):
        self.__rabbit_manager = RabbitMQManager()
        self.__mq_channel = self.__rabbit_manager.setup_consume_channel(self.channel_name, self.__consume_callback)
        self.__mq_channel.start_consuming()

    def __start_mq_consumer(self):
        t = threading.Thread(target=self.__async_consuming)
        t.start()
        return t

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(DistributedSpider, cls).from_crawler(crawler, *args, **kwargs)
        from scrapy import signals
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def __get_from_queue(self):
        item = self.queue.get(timeout=self.__mq_timeout)
        yield item

    def __item_to_request(self, item):
        if item['mark'] not in self.__callback_map.keys():
            raise RuntimeError('Can not find parse callback function of mark key {}'
                               '(Call set_parse_callback method for setting ?) '.format(item['mark']))
        return scrapy.Request(url=item['data'], callback=self.__callback_map[item['mark']])

    def spider_idle(self, spider):
        try:
            for item in self.__get_from_queue():
                self.crawler.engine.crawl(self.__item_to_request(item), self)
            from scrapy.exceptions import DontCloseSpider
            raise DontCloseSpider()
        except queue.Empty:
            self.__mq_channel.stop_consuming()

    def start_requests(self):
        self.__consumer_thread = self.__start_mq_consumer()
        for url in self.start_distributed_requests():
            if not isinstance(url, str):
                raise RuntimeError("start_distributed_requests method "
                                   "should yield type str not {}".format(type(url)))
            item = DistributedSpidersItem()
            item['mark'] = 'root'
            item['data'] = url
            from scrapy.exceptions import DropItem
            try:
                item = self.duplicateFilterPipeline.process_item(item, self)
                self.rabbitMQPipeline.process_item(item, self)
                # self.commit_message(item)
            except DropItem:
                pass
        for item in self.__get_from_queue():
            yield self.__item_to_request(item)

    def __consume_callback(self, ch, method, properties, body):
        data = str(body, encoding='utf-8')
        print("reviced {}".format(body))
        tmp = json.loads(json.loads(data))
        item = DistributedSpidersItem()
        item['mark'] = tmp['mark']
        item['data'] = tmp['data']
        self.__commit_map[item['data']] = (ch, method)
        self.queue.put(item)

    def async_consuming(self):
        self.__mq_channel.start_consuming()

    def start_distributed_requests(self):
        raise NotImplementedError('{}.start_distributed_requests callback is not defined'.format(self.__class__.__name__))

    def set_parse_callback(self, callback_fun, mark_value='root'):
        self.__callback_map[mark_value] = callback_fun
        return self

    def commit_message(self, url):
        if url not in self.__commit_map.keys():
            return
        ch, method = self.__commit_map[url]
        ch.basic_ack(delivery_tag=method.delivery_tag)
