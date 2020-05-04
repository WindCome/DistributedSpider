import json
import time

from DistributedSpiders.frame.Middleware import RedisManager


class DistributedLock:

    # __redis_host = settings.get("REDIS_HOST")
    # __redis_port = settings.get("REDIS_PORT")
    # __redis_host = '121.36.82.230'
    # __redis_port = 6379
    redis_client = RedisManager.get_redis_client()

    # 获取一个分布式锁
    # lock_name：锁定名称
    # acquire_time: 等待获取锁的时间
    # time_out: 锁的超时时间
    @staticmethod
    def acquire_lock(lock_name, identifier, acquire_time=10, time_out=10):
        # identifier = str(uuid.uuid3())
        client = DistributedLock.redis_client
        lock_info = json.dumps({'identifier': identifier, 'count': 1})
        end = time.time() + acquire_time
        lock = "string:lock:" + lock_name
        while acquire_time < 0 or time.time() < end:
            # Redis >=2.6.12
            if client.set(lock, lock_info, ex=time_out, nx=True):
                return True
            current_lock = client.get(lock)
            if current_lock is None:
                continue
            current_lock_info = json.loads(current_lock)
            if current_lock_info is not None:
                if current_lock_info['identifier'] == identifier:
                    current_lock_info['count'] = current_lock_info['count'] + 1
                    if client.set(lock, json.dumps(current_lock_info), ex=time_out, xx=True):
                        return True
            time.sleep(0.001)
        return False

    # 释放一个分布式锁
    @staticmethod
    def release_lock(lock_name, identifier):
        client = DistributedLock.redis_client
        lock = "string:lock:" + lock_name
        pip = client.pipeline(True)
        pip.watch(lock)
        lock_value = pip.get(lock)
        if not lock_value:
            return True
        # tmp = json.loads(lock_value)
        current_lock_info = json.loads(lock_value)
        if current_lock_info['identifier'] == identifier:
            pip.multi()
            current_lock_info['count'] = current_lock_info['count'] - 1
            if current_lock_info['count'] == 0:
                pip.delete(lock)
            else:
                pip.set(lock, json.dumps(current_lock_info), xx=True)
            pip.execute()
            return True
        pip.unwatch()
        return False
