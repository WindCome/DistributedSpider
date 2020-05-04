import re
from lxml import etree

import scrapy


class DoubanSpider(scrapy.Spider):
    name = 'Douban'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def start_requests(self):
        url = "https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=&start={}"
        for i in range(0, 1):
            current_url = url.format(i*20)
            yield scrapy.Request(url=current_url, callback=self.find_info_page)

    def find_info_page(self, response):
        import json
        info = json.loads(response.body.decode(response.encoding))
        data = info["data"]
        for item in data:
            yield scrapy.Request(url=item["url"], callback=self.parse)

    def parse(self, response):
        doc = etree.HTML(response.body.decode(response.encoding))
        star = doc.xpath('//strong[@class="ll rating_num"]')[0].text
        year = doc.xpath('//span[@class="year"]')[0].text
        search = re.search('(\d{4})', year)
        year = search.group(1)
        movie_name = doc.xpath('//span[@class="year"]/preceding-sibling::span[1]')[0].text
        print(movie_name, year, star)

    @staticmethod
    def get_text_of_node(lxml_node):
        stringify = etree.XPath("string()")
        try:
            result = stringify(lxml_node)
        except ValueError:
            result = lxml_node.text
        return result
