import random
import time
from threading import Thread

from DistributedSpiders.frame.Synchronization import DistributedLock

count=20

def seckill(i):
    DistributedLock.acquire_lock('resource',i)
    print("线程:{}--获得了锁".format(i))
    time.sleep(1)
    if random.random() > 0.5:
        print("线程:{} 崩溃了".format(i))
        return
    global count
    DistributedLock.acquire_lock('resource', i)
    if count<1:
        print("线程:{}--没抢到，票抢完了".format(i))
        return
    count-=1
    DistributedLock.release_lock('resource', i)
    print("线程:{}--抢到一张票，还剩{}张票".format(i,count))
    DistributedLock.release_lock('resource',i)


for i in range(10):
    t = Thread(target=seckill,args=(i,))
    t.start()

