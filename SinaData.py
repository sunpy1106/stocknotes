import urllib2

#this is SinaData module
#in this module,you can do two things:
#	1) get all stock number in shanghai stock market and shenzhen stockmarket
#	2) get the exact information of a stock at present


def request(stockId, market):
	market = market.lower()
	stockcode = market + stockId
	url ="http://hq.sinajs.cn/list=%s" %stockcode
	response = urllib2.urlopen(url)
	csvInfo= response.read()
	useInfo = csvInfo[4:]
	exec(useInfo)
	stockInfo = "hq_str_" + stockcode
	stockdata = eval(stockInfo)
	stockdata =  stockdata.split(",")
	return stockdata


def getCurrentData(stockId,market):
	stockData = request(stockId,market)
	return [[stockData[30],stockData[1],stockData[4],stockData[5],stockData[3],stockData[8]]]





