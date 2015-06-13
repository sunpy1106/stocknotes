#!/usr/bin/python
# -*- coding: utf-8 -*-
import SinaData
import YahooData
import StockDataMart
import datetime
import multiprocessing

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
		dataInfo = YahooData.getHistoricalData(stockId,market,'2014-06-01',today)	
	elif maxDate < yesterday :
		dataInfo = YahooData.getHistoricalData(stockId,market,maxDate,yesterday)
	if max(map(len,dataInfo)) > 0:
		updateStockData(stockId,market,stockName,dataInfo)
	maxDate = StockDataMart.getMaxDate(stockId,market)
	if maxDate == yesterday :
		dataInfo = SinaData.getCurrentData(stockId,market)
		if max(map(len,dataInfo)) > 0:
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
	pool_size=multiprocessing.cpu_count()*2
	pool=multiprocessing.Pool(processes=pool_size)
	for rows in stockList:
		stockId = rows[0]
		market = rows[1]
		result = pool.apply_async(stockDataCollection,(stockId,market))
	pool.close()
	pool.join()


