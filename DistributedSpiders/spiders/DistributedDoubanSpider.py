import json
import re
from lxml import etree

from DistributedSpiders.frame.DistributedSpider import DistributedSpider
from DistributedSpiders.items import DistributedSpidersItem


class DistributedDoubanSpider(DistributedSpider):

    name = 'Dis_Douban'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        super().set_parse_callback(self.find_info_page) \
            .set_parse_callback(self.parse, mark_value='info')

    def start_distributed_requests(self):
        url = "https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=&start={}"
        for i in range(0, 1):
            yield url.format(i * 20)

    # def start_requests(self):
    #     url = "https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=&start={}"
    #     for i in range(0, 1):
    #         yield url.format(i * 20)

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
