# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import uuid

from scrapy.conf import settings
from scrapy.exceptions import DropItem

from DistributedSpiders.frame.Middleware import RabbitMQManager, RedisManager, MongoManager
from DistributedSpiders.frame.Synchronization import DistributedLock
from DistributedSpiders.items import DistributedSpidersItem
from DistributedSpiders.utils.JSONUtils import ItemEncoder


class DistributedspidersPipeline(object):
    def process_item(self, item, spider):
        print(item)
        return item


class DuplicateFilterPipeline(object):
    # 分布式去重

    def __init__(self):
        self.__redis_client = RedisManager.get_redis_client()
        self.__delay = settings.get("DOWNLOAD_DELAY")
        self.__identifier = str(uuid.uuid1())

    def open_spider(self, spider):
        spider.duplicateFilterPipeline = self

    def close_spider(self, spider):
        pass

    def process_item(self, item, spider):
        if not isinstance(item, DistributedSpidersItem):
            return item
        fingerprint = DuplicateFilterPipeline.__get_url_fingerprint(item['data'])
        locked = DistributedLock.acquire_lock(fingerprint, self.__identifier,
                                              acquire_time=-1, time_out=5 * self.__delay)
        if not locked:
            raise DropItem('drop {} because of getting lock failure'.format(json.dumps(item, cls=ItemEncoder)))
        duplicate_json_info = self.__redis_client.get(fingerprint)
        if duplicate_json_info is not None:
            duplicate_info = json.loads(duplicate_json_info)
            for x in duplicate_info:
                if x['url'] == item['data']:
                    DistributedLock.release_lock(fingerprint, identifier=self.__identifier)
                    raise DropItem('drop {} because of duplicate'.format(json.dumps(item, cls=ItemEncoder)))
        return item

    def process_item_succeed(self, item):
        if not isinstance(item, DistributedSpidersItem) or item['mark'] == 'data':
            return
        fingerprint = DuplicateFilterPipeline.__get_url_fingerprint(item['data'])
        duplicate_json_info = self.__redis_client.get(fingerprint)
        if duplicate_json_info is None:
            self.__redis_client.set(fingerprint, json.dumps([{'url': item['data'], 'identifier': self.__identifier}]),
                                    ex=30 * self.__delay)
        else:
            duplicate_info = json.loads(duplicate_json_info)
            for x in duplicate_info:
                if x['url'] == item['data']:
                    DistributedLock.release_lock(fingerprint, identifier=self.__identifier)
                    raise RuntimeError('find duplicate_info {} after process'.format(json.dumps(item, cls=ItemEncoder)))
            duplicate_info.append({{'url': item['data'], 'identifier': self.__identifier}})
            self.__redis_client.set(fingerprint, json.dumps(duplicate_info),
                                    ex=30 * self.__delay)
        DistributedLock.release_lock(fingerprint, identifier=self.__identifier)

    @staticmethod
    def __get_url_fingerprint(url):
        import hashlib
        m = hashlib.md5()
        m.update(url.encode())
        return m.hexdigest()


class RabbitMQPipeline(object):
    # RabbitMQ

    def __init__(self):
        self.__channel_map = {}
        self.__rabbit_manager = RabbitMQManager()

    def __get_channel_by_queue_name(self, queue_name):
        if queue_name not in self.__channel_map.keys():
            channel = self.__rabbit_manager.new_channel(queue_name)
            self.__channel_map[queue_name] = channel
        return self.__channel_map[queue_name]

    def open_spider(self, spider):
        spider.rabbitMQPipeline = self

    def close_spider(self, spider):
        self.__rabbit_manager.close_connection(
            self.__get_channel_by_queue_name(spider.channel_name))

    def process_item(self, item, spider):
        if isinstance(item, DistributedSpidersItem) and item['mark'] != 'data':
            # print('[x] Sent'+json.dumps(item, cls=ItemEncoder))
            queue_name = spider.channel_name
            self.__get_channel_by_queue_name(queue_name) \
                .basic_publish(exchange='', routing_key=queue_name, body=json.dumps(item, cls=ItemEncoder))
            spider.duplicateFilterPipeline.process_item_succeed(item)
        return item


class MongoPipeline(object):
    # MongoDB

    def __init__(self):
        self.__mongo_client = MongoManager.get_mongo_client()
        self.__db_data_name = None
        self.__collection_name = None
        self.__record_name = "spider_record"
        self.__db = None
        self.__col_data = None
        self.__col_record = None

    def open_spider(self, spider):
        self.__db_data_name = spider.get_config_by_key_default("mongo_db", "spider_data")
        self.__collection_name = spider.get_config_by_key_default("mongo_col", spider.name)
        self.__db = self.__mongo_client[self.__db_data_name]
        self.__col_data = self.__db[self.__collection_name]
        self.__col_record = self.__db[self.__record_name]

    def close_spider(self, spider):
        pass

    def process_item(self, item, spider):
        if isinstance(item, DistributedSpidersItem) and item['mark'] == 'data':
            with self.__mongo_client.start_session() as s:
                s.start_transaction()
                try:
                    data = json.loads(item['data'])
                    res = self.__col_data.insert_one(data)
                    record = {'task_id': spider.task_id,
                              'process_name': spider.p_name,
                              'data_id': res.inserted_id}
                    self.__col_record.insert_one(record)
                except:
                    # 操作异常，中断事务
                    s.abort_transaction()
                else:
                    s.commit_transaction()
                finally:
                    s.end_session()
        return item
