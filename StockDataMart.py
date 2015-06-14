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




def getStockPriceAndVolume(stockId,market,data_date):
	dbconn = MySQLdb.connect(host="localhost",user="root",passwd="liufei302",db="stockdata",charset='utf8')
	res = dbconn.cursor()
	try:
		res.execute("select closePrice,volume,data_date from t_stock_detail where volume <> 0 and  stockId=%s and market = %s and data_date<= %s group by data_date order by data_date desc " , (stockId,market,data_date))
		dateList = res.fetchall()
	except MySQLdb.Error,e:
		print "Error %d:%s in getStockPriceAndVolume" % (e.args[0],e.args[1])
		sys.exit(1)
	dbconn.close()
	return dateList

def addPriceVolumeDown(stockId,market,stockName,data_date,lastDays):
	dbconn = MySQLdb.connect(host="localhost",user="root",passwd="liufei302",db="stockdata",charset='utf8')
	res = dbconn.cursor()
	res.execute('SET NAMES utf8')
	try:
		res.execute("""insert into t_stock_priceVolumeDown values (%s,%s,%s,%s,%s)""",(stockId,market,stockName,data_date,lastDays))
		dbconn.commit()
	except MySQLdb.Error,e:
		print "Error %d:%s in addPriceVolumeDown" % (e.args[0],e.args[1])
		dbconn.rollback()
	dbconn.close()


def getStockPrice(stockId,market,data_date):
	dbconn = MySQLdb.connect(host="localhost",user="root",passwd="liufei302",db="stockdata",charset='utf8')
	res = dbconn.cursor()
	try:
		res.execute("select closePrice,data_date from t_stock_detail where volume <> 0 and  stockId=%s and market = %s and data_date<= %s group by data_date order by data_date desc " , (stockId,market,data_date))
		dateList = res.fetchall()
	except MySQLdb.Error,e:
		print "Error %d:%s in getStockPrice" % (e.args[0],e.args[1])
		sys.exit(1)
	dbconn.close()
	return dateList

def addStockPriceDown(stockId,market,stockName,data_date,lastDays):
	dbconn = MySQLdb.connect(host="localhost",user="root",passwd="liufei302",db="stockdata",charset='utf8')
	res = dbconn.cursor()
	res.execute('SET NAMES utf8')
	try:
		res.execute("""insert into t_stock_priceDown values (%s,%s,%s,%s,%s)""",(stockId,market,stockName,data_date,lastDays))
		dbconn.commit()
	except MySQLdb.Error,e:
		print "Error %d:%s in addStockPriceDown" % (e.args[0],e.args[1])
		dbconn.rollback()
	dbconn.close()



def getStockVolume(stockId,market,data_date):
	dbconn = MySQLdb.connect(host="localhost",user="root",passwd="liufei302",db="stockdata",charset='utf8')
	res = dbconn.cursor()
	try:
		res.execute("select volume,data_date from t_stock_detail where volume <> 0 and  stockId=%s and market = %s and data_date<= %s group by data_date order by data_date desc " , (stockId,market,data_date))
		dateList = res.fetchall()
	except MySQLdb.Error,e:
		print "Error %d:%s in getStockVolume" % (e.args[0],e.args[1])
		sys.exit(1)
	dbconn.close()
	return dateList



def addVolumeDown(stockId,market,stockName,data_date,lastDays):
	dbconn = MySQLdb.connect(host="localhost",user="root",passwd="liufei302",db="stockdata",charset='utf8')
	res = dbconn.cursor()
	res.execute('SET NAMES utf8')
	try:
		res.execute("""insert into t_stock_volumeDown values (%s,%s,%s,%s,%s)""",(stockId,market,stockName,data_date,lastDays))
		dbconn.commit()
	except MySQLdb.Error,e:
		print "Error %d:%s in addStockVolumeDown" % (e.args[0],e.args[1])
		dbconn.rollback()
	dbconn.close()

def addEvenPrice(stockId,market,stockName,count_days,evenPrice,data_date):
	dbconn = MySQLdb.connect(host="localhost",user="root",passwd="liufei302",db="stockdata",charset='utf8')
	res = dbconn.cursor()
	res.execute('SET NAMES utf8')
	try:
		res.execute("""insert into t_stock_evenPrice values (%s,%s,%s,%s,%s,%s)""",(stockId,market,stockName,evenPrice,data_date,lastDays))
		dbconn.commit()
	except MySQLdb.Error,e:
		print "Error %d:%s in addEvenPrice" % (e.args[0],e.args[1])
		dbconn.rollback()
	dbconn.close()


def addEvenPriceDiff(stockId,market,stockName,count_days,evenSquareDiff,data_date):
	dbconn = MySQLdb.connect(host="localhost",user="root",passwd="liufei302",db="stockdata",charset='utf8')
	res = dbconn.cursor()
	res.execute('SET NAMES utf8')
	try:
		res.execute("""insert into t_stock_evenPriceDiff values (%s,%s,%s,%s,%s,%s)""",(stockId,market,stockName,evenPrice,data_date,lastDays))
		dbconn.commit()
	except MySQLdb.Error,e:
		print "Error %d:%s in addEvenPrice" % (e.args[0],e.args[1])
		dbconn.rollback()
	dbconn.close()


def getEventPriceDiff(stockId,market,count_days,data_date):
	dbconn = MySQLdb.connect(host="localhost",user="root",passwd="liufei302",db="stockdata",charset='utf8')
	res = dbconn.cursor()
	try:
		res.execute("select evenSquareDiff,data_date from t_stock_evenSquareDiff where volume <> 0 and  stockId=%s and market = %s and data_date<= %s group by data_date order by data_date desc " , (stockId,market,data_date))
		dateList = res.fetchall()
	except MySQLdb.Error,e:
		print "Error %d:%s in getEvenSquareDiff" % (e.args[0],e.args[1])
		sys.exit(1)
	dbconn.close()
	return dateList






