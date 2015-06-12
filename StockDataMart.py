#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import MySQLdb
import sys


def addStockData(stockId,market,stockName,openPrice,closePrice,maxPrice,minPrice,volume,dataDate):
	dbconn = MySQLdb.connect(host="localhost",user="root",passwd="liufei302",db="stockdata",charset='utf8')
	res = dbconn.cursor()
	res.execute('SET NAMES utf8')
	try:
		res.execute("""insert into t_stock_detail values (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(stockId,market,stockName,openPrice,closePrice,maxPrice,minPrice,volume,dataDate))
		dbconn.commit()
	except MySQLdb.Error,e:
		print "Error %d:%s in addStockData" % (e.args[0],e.args[1])
		dbconn.rollback()
	dbconn.close()			

def getStockName(stockId,market):
	dbconn = MySQLdb.connect(host="localhost",user="root",passwd="liufei302",db="stockdata",charset='utf8')
	res = dbconn.cursor()
	try:
		res.execute("select stockName from t_stock_slist where stockId=%s and market = %s " , (stockId,market))
		nameList = res.fetchall()
		for dt in nameList:
			stockName = dt[0]
	except MySQLdb.Error,e:
		print "Error %d:%s in getStockName" % (e.args[0],e.args[1])
		sys.exit(1)
	dbconn.close()	
	return stockName



def getMaxDate(stockId,market):
	dbconn = MySQLdb.connect(host="localhost",user="root",passwd="liufei302",db="stockdata",charset='utf8')
	res = dbconn.cursor()
	try:
		res.execute("select max(data_date) from t_stock_detail where stockId=%s and market = %s " , (stockId,market))
		dateList = res.fetchall()
		for dt in dateList:
			maxDate = dt[0]
	except MySQLdb.Error,e:
		print "Error %d:%s in getMaxDate" % (e.args[0],e.args[1])
		sys.exit(1)
	dbconn.close()	
	return maxDate



def getAllStocks():
	dbconn = MySQLdb.connect(host="localhost",user="root",passwd="liufei302",db="stockdata",charset='utf8')
	res = dbconn.cursor()
	try:
		res.execute("select * from t_stock_slist ")
		stockList = res.fetchall()
	except MySQLdb.Error,e:
		print "Error %d:%s in getAllStocks" % (e.args[0],e.args[1])
		sys.exit(1)
	dbconn.close()
	return stockList



