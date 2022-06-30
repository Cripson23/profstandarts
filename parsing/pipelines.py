# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

from parsing.items import ClassGroupsItem, EconomicActivitiesItem, FgosItem, OpkItem, ProfStandartItem, OtfItem, TfItem, UkItem
import pymongo
import logging
from scrapy.exceptions import DropItem

from scrapy.utils.project import get_project_settings
settings = get_project_settings()


class SimplePipeLine(object):
    direction = None

    def __init__(self):
        self.mongo_uri = settings['MONGO_URI']
        self.mongo_db = settings['MONGO_DATABASE']
        
    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        # collections = ['prof_standarts', 'class_groups', 'economic_activities', 'otfs', 'tfs']
        # if self.direction == 'all':
        #     # Очистка всех коллекций
        #     for collection_name in collections:
        #         self.db[collection_name].remove()
        # else:
        #     for collection_name in collections:
        #         self.db[collection_name].remove()

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        find_duplicate = None
        add_info = None
        collection_name = None
        if isinstance(item, ProfStandartItem):
            collection_name = 'prof_standarts'
            add_info = f"ПС: {item['code']}"
            find_duplicate = self.db[collection_name].find_one({'code': item['code']})
        elif isinstance(item, ClassGroupsItem):
            collection_name = 'class_groups'
            add_info = f"Группа занятий: {item['okz_code']}"
            find_duplicate = self.db[collection_name].find_one({'okz_code': item['okz_code']})
        elif isinstance(item, EconomicActivitiesItem):
            collection_name = 'economic_activities'
            add_info = f"Вид экон. активности: {item['okved_code']}"
            find_duplicate = self.db[collection_name].find_one({'okved_code': item['okved_code']})
        elif isinstance(item, OtfItem):
            collection_name = 'otfs'
            add_info = f"ПС: {item['ps_code']} | ОТФ: {item['code']}"
            find_duplicate = self.db[collection_name].find_one({'ps_code': item['ps_code'], 'code': item['code']})
        elif isinstance(item, TfItem):
            collection_name = 'tfs'
            add_info = f"ПС: {item['ps_code']} | ОТФ: {item['otf_code']} | ТФ: {item['code']}"
            find_duplicate = self.db[collection_name].find_one({'ps_code': item['ps_code'], 'otf_code': item['otf_code'], 'code': item['code']})

        if find_duplicate:
            find_duplicate.pop('_id')
            if find_duplicate == item:
                logging.info(f"{add_info} [ALREADY] in db")
            else:
                self.db[collection_name].delete_one(find_duplicate)
                self.db[collection_name].insert(dict(item))
                logging.info(f"{add_info} [UPDATE] in db")
        else:
            self.db[collection_name].insert(dict(item))
            logging.info(f"{add_info} [ADD] to db")
        return item


class FgosPipeLine(object):
    def __init__(self):
        self.mongo_uri = settings['MONGO_URI']
        self.mongo_db = settings['MONGO_DATABASE']
        
    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

        # collections = ['fgos', 'uk', 'opk']
        # Очистка всех коллекций
        # for collection_name in collections:
        #     self.db[collection_name].remove()

    def close_spider(self, spider):
        self.client.close()
    
    def process_item(self, item, spider):
        find_duplicate = None
        add_info = None
        collection_name = None
        if isinstance(item, FgosItem):
            collection_name = 'fgos'
            add_info = f"ФГОС: {item['code']}"
            find_duplicate = self.db[collection_name].find_one({'code': item['code']})
        elif isinstance(item, UkItem):
            collection_name = 'uk'
            add_info = f"Универсальная комптенция: {item['code']}"
            find_duplicate = self.db[collection_name].find_one({'code': item['code']})
        elif isinstance(item, OpkItem):
            collection_name = 'opk'
            add_info = f"Общепрофессиональная компетенция: {item['code']}"
            find_duplicate = self.db[collection_name].find_one({'code': item['code']})

        if find_duplicate:
            find_duplicate.pop('_id')
            if find_duplicate == item:
                logging.info(f"{add_info} [ALREADY] in db")
            else:
                self.db[collection_name].delete_one(find_duplicate)
                self.db[collection_name].insert(dict(item))
                logging.info(f"{add_info} [UPDATE] in db")
        else:
            self.db[collection_name].insert(dict(item))
            logging.info(f"{add_info} [ADD] to db")
        return item