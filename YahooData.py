#!/usr/bin/env python
#
#  Copyright (c) 2007-2008, Corey Goldberg (corey@goldb.org)
#
#  license: GNU LGPL
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.


import urllib2
import sys
import datetime

def getHistoricalData(stockId,market,startDate,endDate):
	if market == 'sh':
		symbol =  '%s.SS' % (stockId )
	elif market == 'sz':
		symbol = stockId +  '.' +  market
	else :
		return 
	print type(startDate)
	print type(endDate)
	if isinstance(startDate,str):
		startDate = datetime.datetime.strptime(startDate, "%Y-%m-%d").strftime('%Y%m%d')
	else:
		startDate = startDate.strftime('%Y-%m-%d')
		startDate = datetime.datetime.strptime(startDate, "%Y-%m-%d").strftime('%Y%m%d')
	if isinstance(endDate,str):
		endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d").strftime('%Y%m%d')
	else:
		endDate = endDate.strftime('%Y-%m-%d')
		endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d").strftime('%Y%m%d')
	print startDate
	print endDate
	hData = get_historical_prices(symbol,startDate,endDate)
	return hData


    
def get_historical_prices(symbol, start_date, end_date):
	url = 'http://ichart.yahoo.com/table.csv?s=%s&' % symbol + \
			'd=%s&' % str(int(end_date[4:6]) - 1) + \
			'e=%s&' % str(int(end_date[6:8])) + \
			'f=%s&' % str(int(end_date[0:4])) + \
			'g=d&' + 'a=%s&' % str(int(start_date[4:6])-1) + \
			'b=%s&' % str(int(start_date[6:8])) + \
			'c=%s&' % str(int(start_date[0:4])) + \
			'ignore=.csv'
	print url

	user_agent = 'Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.9.2.10) Gecko/20100915 Ubuntu/10.04 (lucid) Firefox/3.6.10'
	request = urllib2.Request(url)
	request.add_header('User-agent', user_agent )
	days = urllib2.urlopen(request).readlines()
	data = [day[:-2].split(',') for day in days]
	data = data[1:]
	return data

