#!/usr/bin/python
# -*- coding: utf-8 -*-
import SinaData
import threadpool
import YahooData
import StockDataMart
import datetime

def stockDataCollection(stockId,market):
	maxDate = StockDataMart.getMaxDate(stockId,market)
	stockName = StockDataMart.getStockName(stockId,market)
	today = datetime.date.today()
	yesterday = datetime.date.fromordinal(datetime.date.today().toordinal()-1)
	print maxDate
	print stockName
	print today
	print yesterday
	if maxDate is None :
		dataInfo = YahooData.getHistoricalData(stockId,market,'2015-06-05',today)	
	elif maxDate < yesterday :
		dataInfo = YahooData.getHistoricalData(stockId,market,maxDate,yesterday)
	elif maxDate < today :
		dataInfo = SinaData.getCurrentData(stockId,market)
	else :
		return

	print dataInfo
	print type(dataInfo)
	updateStockData(stockId,market,stockName,dataInfo)

def  updateStockData(stockId,market,stockName,dataInfo):
	for curData in dataInfo:
		dataDate = curData[0]
		openPrice = curData[1]
		maxPrice = curData[2]
		minPrice = curData[3]
		closePrice = curData[4]
		volume = curData[5]
		StockDataMart.addStockData(stockId,market,stockName,openPrice,closePrice,maxPrice,minPrice,volume,dataDate)


def dataCollection():
	stockList = StockDataMart.getAllStocks()
	pool = threadpool.ThreadPool(10)
	reqs = threadpool.makeRequest(stockDataCollection,stockList)	
	[pool.putRequest(req)  for req in reqs]
	pool.wait()


