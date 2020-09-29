# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json


class JumboparserPipeline:

    def open_spider(self, spider):
        self.file = open('results.json', 'w', encoding='utf-8')
        self.file.write('[ \n')

    def close_spider(self, spider):
        self.file.close()
        self.file.write(']')

    def process_item(self, item, spider):
        line = json.dumps(item, ensure_ascii=False).encode('utf-8')
        self.file.write(line.decode() + ",\n")
        return item
