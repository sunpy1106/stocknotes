#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import MySQLdb
import sys
import numpy as np
from scipy.optimize import leastsq

def priceVolumeDown(data_date):
	stockList = StockDataMart.getAllStocks()
	for rows in stockList:
		stockId = rows[0]
		market = rows[1]
		stockName = rows[2]
		dataInfo = StockDataMart.getStockPriceAndVolume(stockId,market,data_date)
		if len(dataInfo) <= 0 :
			continue
		lastDays = 0
		for i in range( 0,len(dataInfo)-1 ):
			if rows[i][0] < rows[i+1][0] and rows[i][1]< rows[i+1][1] then
				lastDays = lastDays + 1
			else:
				break
		if lastDays > 0 :
			StockDataMart.addPriceVolumeDown(stockId,market,stockName,data_date,lastDays)



def priceDown(data_date):
	stockList = StockDataMart.getAllStocks()
	for rows in stockList:
		stockId = rows[0]
		market = rows[1]
		stockName = rows[2]
		dataInfo = StockDataMart.getStockPrice(stockId,market,data_date)
		if len(dataInfo) <= 0 :
			continue
		lastDays = 0
		for i in range( 0,len(dataInfo)-1 ):
			if rows[i][0] < rows[i+1][0]  then
				lastDays = lastDays + 1
			else:
				break
		if lastDays > 0 :
			StockDataMart.addPriceDown(stockId,market,stockName,data_date,lastDays)


def volumeDown(data_date):
	stockList = StockDataMart.getAllStocks()
	for rows in stockList:
		stockId = rows[0]
		market = rows[1]
		stockName = rows[2]
		dataInfo = StockDataMart.getStockVolume(stockId,market,data_date)
		if len(dataInfo) <= 0 :
			continue
		lastDays = 0
		for i in range( 0,len(dataInfo)-1 ):
			if rows[i][0] < rows[i+1][0]  then
				lastDays = lastDays + 1
			else:
				break
		if lastDays > 0 :
			StockDataMart.addVolumeDown(stockId,market,stockName,data_date,lastDays)


def getEvenPrice(data_date,count_days):
	stockList = StockDataMart.getAllStocks()
	for rows in stockList:
		stockId = rows[0]
		market = rows[1]
		stockName = rows[2]
		dataInfo = StockDataMart.getStockPrice(stockId,market,data_date)
		num = 0
		priceSum = 0.0
		for rows in dataInfo:
			curPrice = rows[0]
			priceSum = priceSum + curPrice
			num = num + 1
			if (num >= count_days):
				break
		evenPrice = priceSum / count_days
		StockDataMart.addEvenPrice(stockId,market,stockName,count_days,evenPrice,data_date)


def getEvenPriceDiff(data_date,count_days):
	stockList = StockDataMart.getAllStocks()
	for rows in stockList:
		stockId = rows[0]
		market = rows[1]
		stockName = rows[2]
		dataInfo = StockDataMart.getStockPrice(stockId,market,data_date)
		sumSquareDiff  = 0.0
		evenPrice = StockDataMart.getEvenPrice(stockId,market,data_date,count_days)
		num = 0
		for rows in dataInfo:
			curPrice = rows[0]
			curData = rows[1]
			sumSquareDiff = sumSquareDiff + pow(curPrice - evenPrice,2)
			num = num + 1
			if num >= count_days :
				break
		evenSquareDiff = sumSquareDiff / count_days
		StockDataMart.addEvenPriceDiff(stockId,market,stockName,count_days,evenSquareDiff,data_date)

	
def  evenPriceDiffByLSM(data_date,count_days):
	stockList = StockDataMart.getAllStocks()
	for rows in stockList:
		stockId = rows[0]
		market = rows[1]
		stockName = rows[2]
		dataInfo = StockDataMart.getEvenPriceDiff(stockId,market,count_days,data_date)
		num = 0
		evenPriceDiffList = []
		xList = []
		for rows in dataInfo:
			evenPriceDiff = rows[0]
			evenPriceDiffList.append(evenPriceDiff)
			num = num + 1
			xList.append(num )
			if num >= count_days:
				break
		evenPriceDiffList.reverse()
		xIndexList = np.array(xList,dtype=float)
		yEvenPriceDiffList = np.array(evenPriceDiffList,dtype=float)
		r = leastsq(fitResiduals,[1,1],args=(xIndexList,yEvenPriceDiffList))
		print r[0]


def fitFunction(x,p):
	a,b  = p
	return a * x + b


def fitResiduals(p,x,y):
	return fitFunction(x,p) - y




