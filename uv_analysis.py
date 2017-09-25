import pymysql as psql
import pandas as pd
import numpy as np
import math
import json
import matplotlib.pyplot as plt
import datetime
import pymysql.cursors

class sqlServer(object):  # 定义sqlServer类，用来连接到mysql获取数据
    """docstring for sqlServer"""

    def __init__(self, server={
        'host': '121.40.211.161',
        'port': 58799,
        'user': 'hhh',
        'password': '123456',
        'db': 'UV',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor,
    }, table='`TOTAL_INFO_T`'):
        self.server = server
        self.table = table
        self.allData = self.sqlGetData(sql="SELECT * FROM "+self.table)

    def sqlGetData(self, sql):
        connection = pymysql.connect(**self.server)
        try:
            with connection.cursor() as cursor:
                # 执行sql语句，进行查询
                cursor.execute(sql)
                # 获取查询结果
                result = cursor.fetchall()
            # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
            connection.commit()

        finally:
            connection.close()
        df = pd.DataFrame(result)
        return df

# 比较date1和date2的指定列


def getWowg(date1, date2, columns):
    allHosid = np.union1d(date1.index, date2.index)
    date1All = date1.reindex(allHosid, fill_value=0)
    date2All = date2.reindex(allHosid, fill_value=0)
    uvAllDay = pd.merge(date1All, date2All,
                        left_index=True, right_index=True).drop(0)
    for column in columns:
        compareColumInput = [column+'_x', column+'_y']
        compareColumOutput = 'wowg_'+column
        wowg = (
            uvAllDay[compareColumInput[0]]-uvAllDay[compareColumInput[1]])/uvAllDay[compareColumInput[1]]
        uvAllDay[compareColumOutput] = wowg
    return uvAllDay

# 获取x天之前的日期


def getDate(x):
    today = datetime.date.today()
    oneday = datetime.timedelta(days=x)
    day = today-oneday
    return day

# 提取n天和n+r天前的数据


def getDateData(allData, n, r):
    d1 = n
    d2 = d1+r
    day1 = getDate(d1)
    day2 = getDate(d2)
    uvDay1 = allData[allData['log_date'] == day1].reset_index(
        drop=True).set_index('hos_id').drop('log_date', axis=1)
    uvDay2 = allData[allData['log_date'] == day2].reset_index(
        drop=True).set_index('hos_id').drop('log_date', axis=1)
    return uvDay1, uvDay2

# 显示数据，column表示要检查的字段，limit表示阈值


def showData(data, column, limit, total, data400):
    uvAllDay = data[[column+'_x', column+'_y', 'wowg_'+column]]
    table = pd.merge(uvAllDay[
        uvAllDay['wowg_'+column] < limit], total, left_index=True, right_index=True, how='left')
    tableShow = pd.merge(table, data400, left_on='Hospital',
                         right_on=r'医院名称', how='left')
    return tableShow

# 获取day天内的400报修信息


def get400Data(path, r):
    day = getDate(r)
    data400 = pd.read_excel(path)
    data400.columns = data400.loc[0].values.tolist()
    data400 = data400.drop(0)
    data4001 = pd.DataFrame([])
    data4001[[r'报修日期', r'解决问题日期']] = data400[
        [r'报修日期', r'解决问题日期']].dropna().astype('datetime64[ns]')
    data4001[[r'医院名称', r'报修问题', r'解决方式', r'解决人']] = data400[
        [r'医院名称', r'报修问题', r'解决方式', r'解决人']]
    return data4001[(data4001[r'报修日期'] > day)]


# 获取uv原始数据和总表数据
def getRawData(server={
        'host': '121.40.211.161',
        'port': 58799,
        'user': 'hhh',
        'password': '123456',
        'db': 'hive_statistical_results',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor},
        table="`funnel_model2`"):
    uvServer = sqlServer(server, table)
    uvData = uvServer.allData
    totalServer = sqlServer()
    totalInfo = totalServer.allData
    totalInfo1 = totalInfo[['Hospital', 'CurrentBroadbandBandwidth', 'BroadbandType',
                            'GatewayVendor', 'GatewayType', 'ApVendor', 'ApType', 'ApVolume', 'GWID', 'GWIP']].drop(0)
    totalInfo1['hos_id'] = totalInfo['HosID'].drop(0).astype('int32')
    totalInfo1 = totalInfo1.set_index('hos_id')
    return uvData, totalInfo1

# 清洗数据
def dataClean(uvData):
    uvData = uvData.replace('\\N', 0).fillna(0).sort_values(
        by='log_date', ascending=False).reset_index(drop=True)
    uvDataGood = uvData[['hos_id', 'dhcp', 'portal', 'prelogin',
                         'login', 'webforward', 'hardforward']].astype('int32')
    uvDataGood['log_date'] = uvData['log_date'].astype('datetime64[ns]')
    uvDataGood['forward_rate'] = uvDataGood['hardforward']/uvDataGood['dhcp']
    uvDataLite = uvDataGood.drop(
        ['portal', 'prelogin', 'login', 'webforward'], axis=1)
    return uvDataGood

def analysis(interestedColumns, uvDataGood,totalInfo,day=1, cycle=7, limit=-0.4):
    analysisList = {}
    columnName = {''}
    columns = interestedColumns
    path = r'/root/ipython/400与回访医院报修记录单统计表 (更改版).xlsx'
    uvDay1, uvDay2 = getDateData(uvDataGood, day, cycle)
    data400 = get400Data(path, day+cycle)
    uvAllDay = getWowg(uvDay1, uvDay2, columns)
    for column in columns:
        outPut = showData(uvAllDay, column, limit, totalInfo, data400).set_index("Hospital")
        outPut[r'现象'] = str(column) + '<' + str(limit)
        analysisList[column] = outPut
    return analysisList

def main():
    # 定义初始参数
    interested = ['forward_rate', 'dhcp', 'login']
    server = {
            'host': '121.40.211.161',
            'port': 58799,
            'user': 'hhh',
            'password': '123456',
            'db': 'hive_statistical_results',
                  'charset': 'utf8mb4',
                  'cursorclass': pymysql.cursors.DictCursor,
        }
    table = '`funnel_model2`'
    uvData, totalInfo = getRawData(server, table)
    uvDataGood1 = dataClean(uvData)
    a = analysis(interested,uvDataGood1,totalInfo,1,1*7)
    b = analysis(interested,uvDataGood1,totalInfo,1,4*7)
    pathList1 = []
    pathList4 = []
    for x in a :
        path1 = r'/root/ipython/UV分析结果/一周对比/' + x+'/一周对比_'+ str(getDate(1))+'_'+x + '.xlsx'
        a[x].to_excel(path1)
        pathList1.append(path1)
    for x in b :
        path1 = r'/root/ipython/UV分析结果/四周对比/' + x+'/四周对比_'+ str(getDate(1))+'_'+x + '.xlsx'
        b[x].to_excel(path1)
        pathList4.append(path1)

if __name__ == '__main__':
    main()
