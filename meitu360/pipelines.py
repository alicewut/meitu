# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy import Request
from scrapy.exceptions import DropItem
from scrapy.pipelines.images import ImagesPipeline

import pymongo
import pymysql

# class Meitu360Pipeline(object):
#     def process_item(self, item, spider):
#         return item

class MongoPipeline(object):
	def __init__(self, mongo_url, mongo_db, mongo_user, mongo_passwd, mongo_port):
		self.mongo_url = mongo_url
		self.mongo_db = mongo_db
		self.mongo_user = mongo_user
		self.mongo_passwd = mongo_passwd
		self.mongo_port = mongo_port

	@classmethod
	def from_crawler(cls, crawler):
		return cls(
			mongo_url = crawler.settings.get('MONGO_URL'),
			mongo_db = crawler.settings.get('MONGO_DB'),
			mongo_user = crawler.settings.get('MONGO_USER'),
			mongo_passwd = crawler.settings.get('MONGO_PASSWD'),
			mongo_port = crawler.settings.get('MONGO_PORT'),
		)
		
	def open_spider(self, spider):
		self.client = pymongo.MongoClient(host=self.mongo_url, port=self.mongo_port, username=self.mongo_user, password=self.mongo_passwd)
		self.db = self.client[self.mongo_db]
	
	
	def process_item(self, item, spider):
		self.db[item.collection].insert(dict(item))
		return item
	
	def close_spider(self, spider):
		self.client.close()
		
		
class MysqlPipline(object):
	
	def __init__(self, mysql_host, mysql_port, mysql_db, mysql_user, mysql_passwd):
		self.mysql_host = mysql_host
		self.mysql_port = mysql_port
		self.mysql_db = mysql_db
		self.mysql_user = mysql_user
		self.mysql_passwd = mysql_passwd
	
	@classmethod
	def from_crawler(cls, crawler):
		return cls(
			mysql_host = crawler.settings.get('MYSQL_HOST'),
			mysql_port = crawler.settings.get('MYSQL_PORT'),
			mysql_db = crawler.settings.get('MYSQL_DB'),
			mysql_user = crawler.settings.get('MYSQL_USER'),
			mysql_passwd= crawler.settings.get('MYSQL_PASSWD')
		)
	
	def process_item(self, item, spider):
		data = dict(item)
		keys = ','.join(data.keys())
		values = ','.join(['%s'] * len(data))
		sql = 'insert into %s (%s) values (%s)' % (item.table, keys, values)
		sql_data = tuple(data.values())
		self.cursor.execute(sql, sql_data)
		self.db.commit()
		return item
		
	def open_spider(self, spider):
		self.db = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user, passwd=self.mysql_passwd, db=self.mysql_db, charset='utf8')
		self.cursor = self.db.cursor()
		
	def close_spider(self, spider):
		self.db.close()
	
class ImagePipeline(ImagesPipeline):
	def file_path(self, request, response=None, info=None):
		url = request.url
		file_name = url.split('/')[-1]
		return file_name
	
	def item_completed(self, results, item, info):
		image_paths = [x['path'] for ok,x in results if ok]
		if not image_paths:
			raise DropItem('Image Downloaded Failed')
		return item
	
	def get_media_requests(self, item, info):
		yield Request(item['url'])