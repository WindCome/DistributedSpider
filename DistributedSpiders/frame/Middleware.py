import pika
import pymongo
import redis
from scrapy.utils.project import get_project_settings
settings = get_project_settings()


class RabbitMQManager:
    def __init__(self):
        self.channels = {}
        self.__rabbit_host = settings.get("MQ_HOST")
        self.__rabbit_port = settings.get("MQ_PORT")

    def new_channel(self, queue_name):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.__rabbit_host, port=self.__rabbit_port))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name)
        self.channels[channel] = connection
        return channel

    def setup_consume_channel(self, queue_name, callback):
        channel = self.new_channel(queue_name)
        # channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
        return channel

    def close_connection(self, channel):
        self.channels[channel].close()


class RedisManager:
    __redis_host = settings.get("REDIS_HOST")
    __redis_port = settings.get("REDIS_PORT")

    __redis_client = redis.Redis(host=__redis_host, port=__redis_port)

    @staticmethod
    def get_redis_client():
        return RedisManager.__redis_client


class MongoManager:
    __mongo_host = settings.get("MONGO_HOST")
    __mongo_port = settings.get("MONGO_PORT")

    __mongo_client = pymongo.MongoClient("mongodb://{}:{}/".format(__mongo_host,__mongo_port))

    @staticmethod
    def get_mongo_client():
        return MongoManager.__mongo_client
