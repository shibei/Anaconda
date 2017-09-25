#!/usr/bin/env python
#!/usr/local/lib/python3.6
# -*- coding: utf-8 -*-
# @Date    : 2017-07-05 20:48:24
# @Author  : mx (shibei1988@foxmail.com)
# @Link    :
# @Version : $Id$


import requests
import json
import time
import xlrd
import xlwt
import pymysql

# def getHcData(url)


class DataList(object):
    """docstring for DataList"""

    def __init__(self, url, manufactor):
        self.url = url
        self.manufactor = manufactor

    def getApiData(self):
        data = []
        re = requests
        if self.manufactor == 'HC':
            data = json.loads(re.get(url=self.url).text)['data']
            for x in data:
                x['manuFacturer'] = 'HC'
                x['date'] = time.strftime(
                    '%Y-%m-%d', time.localtime(time.time()))
                x['clock'] = time.strftime(
                    '%H:%M:%S', time.localtime(time.time()))
                if x['type'] == '静态IP':
                    x['type'] = 'static'
                x['downSpeed'] = int(x['downSpeed'])*8
                x['upSpeed'] = int(x['upSpeed'])*8
                x['totalDown'] = int(x['totalDown'])*8
                x['totalUp'] = int(x['totalUp'])*8
        elif self.manufactor == "abloomy":
            for x in range(9999):
                limit, skip = str(50), str((x)*50)
                pageData = json.loads(
                    re.get(url=self.url+'?limit='+limit+'&skip='+skip).text)['data']
                if len(pageData) == 0:
                    break
                data = data+pageData
                if len(pageData) < 50:
                    break
            for x in data:
                x['manuFacturer'] = 'Abloomy'
                x['date'] = time.strftime(
                    '%Y-%m-%d', time.localtime(time.time()))
                x['clock'] = time.strftime(
                    '%H:%M:%S', time.localtime(time.time()))
                # x['down_speed'] = int(x['down_speed'])*8*1024
                # x['up_speed'] = int(x['up_speed'])*8*1024
                # x['total_down'] = int(x['total_down'])*8*1024
                # x['total_up'] = int(x['total_up'])*8*1024
                try:
                    x['down_speed'] = int(x['down_speed'])*8*1024
                except:
                    pass
                try:
                    x['up_speed'] = int(x['up_speed'])*8*1024
                except:
                    pass
                try:
                    x['total_down'] = int(x['total_down'])*8*1024
                except:
                    pass
                try:
                    x['total_up'] = int(x['total_up'])*8*1024
                except:
                    pass
        return data


urlHc = 'http://gbcom.hos-wifi.com:8888/ccsv3/rest/partner/bblink/routers'
urlAbloomy = 'http://112.65.205.107/rest/partner/bblink/routers'

while True:
	try:
	    hcData = DataList(urlHc, 'HC').getApiData()
	except:
	    time.sleep(30)
	try:
	    abloomyData = DataList(urlAbloomy, 'abloomy').getApiData()
	except:
	    time.sleep(30)

	allData = []

	for i in range(len(hcData)):
	    a = [hcData[i][sorted(hcData[i].keys())[x]] for x in range(len(hcData[0]))]
	    allData.append(a)

	for i in range(len(abloomyData)):
	    a = [abloomyData[i][sorted(abloomyData[i].keys())[x]]
	         for x in range(len(hcData[0]))]
	    allData.append(a)

	# print(allData)

	db = pymysql.connect("127.0.0.1", "root", "admin123", "device")
	cursor = db.cursor()

	for x in allData:
	    jsonData = json.dumps(x).strip('[').strip(']')
	    # jsonData.encode('utf-8')
	    sql = 'insert into dev_info values(' + jsonData + ')'
	    try:
	        cursor.execute(sql)
	    except:
	        pass
	    db.commit()
	    # print(jsonData,' ',type(jsonData))
	    print(x)

	db.close()
	time.sleep(600)
