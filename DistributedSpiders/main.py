from scrapy import cmdline

if __name__ == '__main__':
    task_id = "123"
    print("task_id={}".format(task_id))
    cmdline.execute("scrapy crawl Dis_Douban -a task_id={}".format(task_id).split())
