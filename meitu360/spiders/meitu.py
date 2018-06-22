# -*- coding: utf-8 -*-
import scrapy
from urllib.parse import urlencode
from meitu360.items import MeituItem
import json

class MeituSpider(scrapy.Spider):
	name = 'meitu'
	allowed_domains = ['image.so.com']
	
	# start_urls = ['http://image.so.com/']
	max_pages = 50
	
	# def parse(self, response):
	def start_requests(self):
		data = {
			'cn': 'photography',
			'listtype': 'new',
			# 'temp': '1'
		}
		base_url = 'http://images.so.com/zj?'
		for page in range(self.max_pages):
			data['sn'] = 30 * int(page)
			params = urlencode(data)
			full_url = base_url + params
			yield scrapy.Request(full_url, callback=self.parse)
	
	def parse(self, response):
		result = json.loads(response.text)
		for image in result.get('list'):
			item = MeituItem()
			item['id'] = image.get('id')
			item['url'] = image.get('qhimg_url')
			item['thumb'] = image.get('qhimg_thumb_url')
			item['title'] = image.get('group_title')
			# print(item)
			yield item
   
   