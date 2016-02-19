from lianjia import *

def test_second_hand_sale(city=u'\u5317\u4eac'):
	crawler = lianjia(city)
	crawler.crawl_second_hand_sale(1, 1)

def test_apartments(apturl, city=u'\u5317\u4eac'):
	crawler = lianjia(city)
	crawler.crawl_apartments(apturl)