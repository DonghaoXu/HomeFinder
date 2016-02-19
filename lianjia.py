import sqlite3
import urllib2
import bs4 as bs
import re

def crawl_html(url):
	try:
		req = urllib2.Request(url)
		res = urllib2.urlopen(req)
		html = res.read()
		delete_error(url)
		return html
	except:
		print 'Crawler failed at: ' + url
		record_error(url, 'urllib')

def crawl_city():
	# retrieve the html
	url = 'http://www.lianjia.com'
	html = crawl_html(url)

	# make the soup
	soup = bs.BeautifulSoup(html, 'lxml')
	tags = soup.find_all('div', 'city right')[0]
	
	# save to sqlite
	conn = sqlite3.connect('lianjia.sqlite')
	cur = conn.cursor()
	cur.executescript('''
		DROP TABLE IF EXISTS City;

		CREATE TABLE City (
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
			city TEXT,
			url TEXT
		);
	''')
	for city in tags.find_all('a'):
		cname = unicode(city.span.string)
		curl = unicode(city['href'])
		cur.execute('''
			INSERT INTO City (city, url) VALUES (?, ?)''', (cname, curl)
		)
		print cname, ':', curl

	conn.commit()
	conn.close()

def get_city_list():
	conn = sqlite3.connect('lianjia.sqlite')
	cur = conn.cursor()

	cities = dict()
	cur.execute('''
		CREATE TABLE IF NOT EXISTS City (
			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
			city TEXT UNIQUE,
			url TEXT
		)
	''')
	cur.execute('SELECT * FROM City')

	ls = cur.fetchall()
	if len(ls) < 1:
		crawl_city()
		cur.execute('SELECT * FROM City')
		ls = cur.fetchall()

	for (cid, cname, curl) in ls:
		cities[cname] = (cid, curl)
	
	return cities

def record_error(url, err):
	conn = sqlite3.connect('lianjia.sqlite')
	cur = conn.cursor()

	cur.execute('''
		CREATE TABLE IF NOT EXISTS Errors (
			url TEXT NOT NULL PRIMARY KEY UNIQUE,
			errortype TEXT
		)
	''')

	cur.execute('''
		INSERT OR IGNORE INTO Errors
		(url, errortype)
		VALUES (?, ?)''', (url, err))

	conn.commit()
	conn.close()

def delete_error(url):
	conn = sqlite3.connect('lianjia.sqlite')
	cur = conn.cursor()

	cur.execute('''
		CREATE TABLE IF NOT EXISTS Errors (
			url TEXT NOT NULL PRIMARY KEY UNIQUE,
			errortype TEXT
		)
	''')

	cur.execute('''
		DELETE FROM Errors WHERE url = ?''', (url, ))

	conn.commit()
	conn.close()

def get_error_list():
	conn = sqlite3.connect('lianjia.sqlite')
	cur = conn.cursor()

	cur.execute('''
		SELECT * FROM Errors
	''')

	errorlist = dict()
	for (url, err) in cur.fetchall():
		errorlist[url] = err

	conn.close()

	return errorlist


class lianjia:
	def __init__(self, city):
		if not isinstance(city, unicode):
			city = city.decode('utf8')
		self._cname = city
		ls = get_city_list()
		self._cid = ls[city][0]
		self._curl = ls[city][1]

	def crawl_second_hand_sale(self, start_page=None, end_page=None):
		curl = self._curl + 'ershoufang'
		print 'Retrieving ' + curl
		html = crawl_html(curl)

		# make the soup
		soup = bs.BeautifulSoup(html, 'lxml')
		tags = soup.find_all('div', class_='page-box house-lst-page-box')[0]
		
		# Get maximum page number
		maxpage = int(re.findall('''"totalPage":([0-9]*)''', tags['page-data'])[0])
		print 'Maximum page number is', maxpage

		# prepare sqlite
		conn = sqlite3.connect('lianjia.sqlite')
		cur = conn.cursor()
		cur.execute('''
			CREATE TABLE IF NOT EXISTS SecondSale (
				id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
				unitprice INTEGER,
				area NUMERIC,
				orientation TEXT,
				style TEXT,
				story TEXT,
				aptid INTEGER,
				url TEXT UNIQUE NOT NULL
			)'''
		)

		if start_page is None:
			start_page = 1

		if end_page is None:
			end_page = maxpage

		for i in range(start_page, end_page + 1):
			url = curl + '/pg' + str(i)
			print 'Retrieving page ' + str(i) + ': ' + url
			html = crawl_html(url)

			soup = bs.BeautifulSoup(html, 'lxml')
			tags = soup.find_all(id='house-lst')[0]	

			# Crawl the lists
			for li in tags.find_all('li'):
				print 'Parsing html......'
				saleurl = li.h2.a['href']

				# cur.execute('''
				# 	SELECT id FROM SecondSale WHERE url = ?''', (saleurl, ))
				# # Has been crawled
				# if cur.fetchone() is not None:
				# 	continue

				div = li.find_all(class_='col-1')[0]
				apt = div.contents[0].contents[0].span.string
				apturl = div.contents[0].contents[0]['href']
				style = div.contents[0].contents[2].string.strip()
				area = div.contents[0].contents[3].string.strip()
				area = float(re.findall('[0-9.]*', area)[0])
				orientation = div.contents[0].contents[4].string.strip()
				story = div.contents[1].div.contents[2].string.strip()
				try:
					year = int(div.contents[1].div.contents[4].string.strip()[:4])
				except:
					year = None

				div = li.contents[1].contents[2]
				totalprice = int(div.div.span.string.strip()) * 1e+4
				unitprice = totalprice / area

				# check if the apartment has been crawled
				try:
					cur.execute('''
						SELECT id, address FROM Apartments WHERE url = ?''', (apturl, ))
					(aptid, address) = cur.fetchone()
					if address is None:
						raise
				except:
					#if len(ls) < 1 or ls[0][1] is None: # crawl the apartment
					self.crawl_apartments(apturl, apt)
					cur.execute('''
						SELECT id FROM Apartments WHERE url = ?''', (apturl, ))
					aptid = cur.fetchone()[0]
				print '##########Sale Info##########'
				print 'Area:', area
				print 'Unit price:', unitprice
				print 'Year:', year
				print 'Orientation:', orientation
				cur.execute('''
					INSERT OR REPLACE INTO SecondSale
					(id, unitprice, area, orientation, style, story, aptid, url)
					VALUES ((SELECT id FROM SecondSale WHERE url = ?), ?, ?, ?, ?, ?, ?, ?)''',
					(url, unitprice, area, orientation, style, story, aptid, saleurl))
				
				conn.commit()
		conn.close()

	def crawl_apartments(self, apturl, apt=u''):
		print 'Retrieving ' + apt + ': ' + apturl
		html = crawl_html(apturl)

		soup = bs.BeautifulSoup(html, 'lxml')
		tags = soup.find_all(class_='title fl')[0]

		if apt != unicode(tags.a.h1.string):
			apt = unicode(tags.a.h1.string)
		
		address = unicode(tags.find_all(class_='adr')[0].string)
		
		tags = soup.find_all(class_='res-info fr')[0]
		try:
			avgprice = float(tags.find_all(class_='num')[0].contents[0])
			tags = tags.find_all('li')
			year = int(tags[0].span.span.string[:4])
			numbuildings = int(re.findall('[0-9]*', tags[5].find_all(class_='other')[0].string)[0])
			plotratio = float(tags[5].find_all(class_='other')[1].string)
			greenratio = float(re.findall('[0-9]*', tags[6].find_all(class_='other')[1].string)[0]) / 100
		except:
			avgprice = None
			year = None
			numbuildings = None
			plotratio = None
			greenratio = None

		print '##########Apartment Info##########'
		print 'Apartment: ', apt
		print 'Address: ', address
		print 'Average price: ', avgprice
		print 'Year: ', year
		# prepare sqlite
		conn = sqlite3.connect('lianjia.sqlite')
		cur = conn.cursor()
		cur.execute('''
			CREATE TABLE IF NOT EXISTS Apartments (
				id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
				name TEXT NOT NULL,
				address TEXT NOT NULL,
				year INTEGER,
				avgprice REAL,
				numbuildings INTEGER,
				plotratio REAL,
				greenratio REAL,
				url TEXT UNIQUE NOT NULL
			)''')

		cur.execute('''
			INSERT OR REPLACE INTO Apartments
			(id, name, address, year, avgprice, numbuildings, plotratio, greenratio, url)
			VALUES ((SELECT id FROM Apartments WHERE url = ?), ?, ?, ?, ?, ?, ?, ?, ?)''',
			(apturl, apt, address, year, avgprice, numbuildings, plotratio, greenratio, apturl)
		)

		conn.commit()
		conn.close()

	def crawl_second_hand_deal():
		url = self._curl + 'chengjiao'










		