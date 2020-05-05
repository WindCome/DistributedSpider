# 基于RabbitMQ消息队列的分布式爬虫

本项目为2020年《分布式操作系统》课程大作业的一部分

## Python环境
* scrapy
* redis
* pika
* pymongo

```
python -m pip install scrapy
python -m pip install redis
python -m pip install pika
python -m pip install pymongo -i http://pypi.douban.com/simple/ --trusted-host pypi.douban.com
```

## 启动
```
scrapy crawl [spider_name]
```

由于本项目暂时只有一个用于爬取豆瓣的`Dis_Douban`爬虫,
可以直接使用如下命令运行：
```
scrapy crawl Dis_Douban
```

另外可以使用可选参数如下：

* `task_id`:(Optional)指定本次爬虫的任务ID，默认为爬虫名称
* `p_name`:(Optional)指定爬虫进程的名称用于在MongoDB中记录任务信息，默认为MAC地址和PID的字符串拼接
* `mongo_db`:(Optional)用于指定储存爬取结果数据的MongoDB数据库名称，默认为`spider_data`
* `mongo_col`:(Optional)用于指定储存爬取结果数据的集合名称，默认为爬虫名称

使用这些额外的参数时请使用scrapy的`-a`选项,如：
```
scrapy crawl Dis_Douban -a task_id=123
```


