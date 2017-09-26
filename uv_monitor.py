import pymysql as psql
import pandas as pd
import numpy as np
import math
import json
import matplotlib.pyplot as plt
import datetime
import pymysql.cursors
from contextlib import contextmanager
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
from sqlalchemy import create_engine

# 使用上下文管理器来连接数据库的函数


@contextmanager
def connect(server):
    database = pymysql.connect(**server)
    yield database
    database.close()

# 在数据库中执行命令函数


def sqlAction(database, action):
    with database.cursor() as cursor:
        # 执行sql语句，进行查询
        cursor.execute(action)
        # 获取查询结果
        result = cursor.fetchall()
    # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
    database.commit()
    return result


class SqlServer(object):  # 定义sqlServer类，用来连接到mysql获取数据
    """docstring for sqlServer"""
    #   server的数据结构示例
    # {"server": { #数据库信息
    #     'host': 'localhost',#ip地址
    #     'port': 3066,端口号
    #     'user': 'root',用户名
    #     'password': 'password'密码,
    #     'db': 'db',数据库名称
    #     'charset': 'utf8mb4',编码方式
    #     'cursorclass': pymysql.cursors.DictCursor
    # },
    #     "comm": {命令描述和要执行的命令
    #     'apData': "SELECT * FROM `ap_count`",
    #     'uvData': "SELECT * FROM `funnel_model2`"
    # }}

    def __init__(self, server):
        self.server = server["server"]
        self.sqlComm = server["comm"]

#     只执行一条命令
    def getOneData(self, key):
        server = self.server
        with connect(server) as database:
            result = sqlAction(database, self.sqlComm[key])
#             print(result)
        try:
            df = pd.DataFrame(result)
        except:
            df = pd.DataFrame([])
        return df

#     执行所有命令
    def getAllData(self):
        server = self.server
        sqlDatas = {}
        with connect(server) as database:
            for key in self.sqlComm:
                result = pd.DataFrame(sqlAction(database, self.sqlComm[key]))
                sqlDatas[key] = result
        return sqlDatas

# 获取x天之前的日期


def getDate(x):
    today = datetime.date.today()
    oneday = datetime.timedelta(days=x)
    day = today-oneday
    return day

# 重新排序列


def reColumns(df, columns=['hos_id', 'dhcp', 'portal',
                           'prelogin', 'login', 'webforward',
                           'hardforward', 'log_date']):
    for x in range(len(columns)):
        p = df.pop(columns[x])
        df.insert(x, columns[x], p)
    return df


def get400Data(day, cycle, path=r'/root/ipython/400与回访医院报修记录单统计表 (更改版).xlsx'):
    day1 = getDate(day)
    day2 = getDate(day+cycle)
    data400 = pd.read_excel(path)
    data400.columns = data400.loc[0].values.tolist()
    data400 = data400.drop(0)
    data4001 = pd.DataFrame([])
    data4001[[r'报修日期', r'解决问题日期']] = data400[
        [r'报修日期', r'解决问题日期']].dropna().astype('datetime64[ns]')
    data4001[[r'医院名称', r'报修问题', r'解决方式', r'解决人']] = data400[
        [r'医院名称', r'报修问题', r'解决方式', r'解决人']]
    return data4001[(data4001[r'报修日期'] >= day2) & (data4001[r'报修日期'] <= day1)]

# 从文件中获得DB信息


def getDbInfo(file, sheet):
    serverDf = np.transpose(pd.read_excel(file, sheet))
    serverCommList = {}
    for server in serverDf:
        serverComm = {"comm": {}}
        serverDict = serverDf[server].to_dict()
        serverDict['password'] = str(serverDict['password'])
        serverDict['charset'] = 'utf8mb4'
        serverDict['cursorclass'] = pymysql.cursors.DictCursor
        serverComm["server"] = serverDict
        serverCommList[server] = serverComm
    return serverCommList

# 从文件中获得sender信息


def emailSender(file, sheet):
    senderDf = np.transpose(pd.read_excel(file, sheet))
    senderList = {}
    for sender in senderDf:
        senderDict = senderDf[sender].to_dict()
        senderList[sender] = senderDict
    return senderList

# 从文件中获得receiver信息


def emailReceiver(file, sheet):
    receiveDf = pd.read_excel(file, sheet)
    return receiveDf['receive'].tolist()


def getRawData(day, cycle):

    day0 = str(getDate(0))
    day1 = str(getDate(day))
    day2 = str(getDate(day+cycle))

    DbList = getDbInfo(r'/root/ipython/user_list.xlsx', r'db_user')
    uvBase = DbList['uvBase']
    hosBase = DbList['hosBase']
    devBase = DbList['devBase']

    totalInfoFields = json.dumps(['HosID', 'Hospital', 'Value',
                                  'CurrentBroadbandBandwidth', 'GatewayVendor',
                                  'ApVendor', 'ApType', 'GWID']).strip('[').strip(']').replace('"', '')

    devInfoFields = json.dumps(['clock', 'cpuUsed', 'downSpeed',
                                'gwid', 'starttimeLong',
                                'status', 'wanIp']).strip('[').strip(']').replace('"', '')

    uvBase['comm']['apData'] = "SELECT * FROM `ap_count` WHERE \
    `log_date` = '%s' OR `log_date` = '%s' ORDER BY \
    `log_date` DESC" % (day2, day1)

    uvBase['comm']['uvData'] = "SELECT * FROM `funnel_model2` WHERE \
    `log_date` = '%s' OR `log_date` = '%s' ORDER BY \
    `log_date` DESC" % (day2, day1)

    devBase['comm']['devInfo'] = "SELECT %s FROM `dev_info` \
    WHERE `date` = '%s' ORDER BY `clock` DESC LIMIT 0, 1000" % (devInfoFields, day0)

    hosBase['comm']['totalInfo'] = "SELECT %s FROM `TOTAL_INFO_T_%s`" % (
        totalInfoFields, day1.replace('-', ''))

    uvDatabase, hosDatabase, devDatabase = SqlServer(
        uvBase), SqlServer(hosBase), SqlServer(devBase)

    uvDataRaw, apDataRaw, hosInfoRaw, devInfoRaw = uvDatabase.getOneData('uvData'), uvDatabase.getOneData(
        'apData'), hosDatabase.getOneData('totalInfo'), devDatabase.getOneData('devInfo')

    data400Raw = get400Data(day, cycle)
    return uvDataRaw, apDataRaw, hosInfoRaw, devInfoRaw, data400Raw


# 清洗数据
def dataClean(uvData, apData):
    if len(uvData) > 0:
        reColumns(uvData)
        uvData = uvData.replace('\\N', 0).fillna(0).sort_values(
            by='log_date', ascending=False).reset_index(drop=True)
        uvDataGood = uvData[['hos_id', 'dhcp', 'portal', 'prelogin',
                             'login', 'webforward', 'hardforward']].astype('int32')
        uvDataGood['log_date'] = uvData['log_date'].astype('datetime64[ns]')
        uvDataGood['forward_rate'] = uvDataGood[
            'hardforward']/uvDataGood['dhcp']
        uvDataLite = uvDataGood.drop(
            ['portal', 'prelogin', 'login', 'webforward'], axis=1)
    else:
        uvDataGood = uvData

    if len(apData) > 0:
        apData = apData.replace(
            ['福建', '安徽', '福建省', '安徽省', ''], np.nan).dropna()
        apDataGood = apData[['hos_id', 'ap_count']].astype('int64')
        apDataGood['log_date'] = apData['log_date'].astype('datetime64[ns]')
    else:
        apDataGood = apData
    return uvDataGood, apDataGood

# 提取n天和n+r天前的数据


def getDateData(uvData, apData, day=1, cycle=7):
    d1 = day
    d2 = d1+cycle
    day1 = getDate(d1)
    day2 = getDate(d2)
    if len(uvData) > 0:
        uvDay1 = uvData[uvData['log_date'] == day1].reset_index(
            drop=True).set_index('hos_id').drop('log_date', axis=1)
        uvDay2 = uvData[uvData['log_date'] == day2].reset_index(
            drop=True).set_index('hos_id').drop('log_date', axis=1)

    if len(apData) > 0:
        apDay1 = apData[apData['log_date'] == day1].set_index(
            'hos_id').drop('log_date', axis=1)
        apDay2 = apData[apData['log_date'] == day2].set_index(
            'hos_id').drop('log_date', axis=1)
        uvDay1 = pd.merge(uvDay1, apDay1, left_index=True,
                          right_index=True, how='left')
        uvDay2 = pd.merge(uvDay2, apDay2, left_index=True,
                          right_index=True, how='left')
#     print(uvDay1.index, uvDay2.index)
    allHosid = np.union1d(uvDay1.index, uvDay2.index)
    uvDay1 = uvDay1.reindex(allHosid, fill_value=0)
    uvDay2 = uvDay2.reindex(allHosid, fill_value=0)

    if len(apData) > 0:
        uvDay1.insert(0, 'ap_count', uvDay1.pop('ap_count'))
        uvDay2.insert(0, 'ap_count', uvDay2.pop('ap_count'))

    uvDay2Lite = uvDay2[uvDay2['dhcp'] > 200]
    uvDay1Lite = uvDay1.reindex(uvDay2Lite.index)
    return uvDay1, uvDay2, uvDay1Lite, uvDay2Lite

# 依靠经验值来构建的决策树


def decisionTree(data, rawData, apLimit=-0.1, forwardLimit=-0.4, globalLimit=-0.4):
    index = data.index  # 获得目录
    splitTerm0 = data[index[0]] <= globalLimit  # 分裂条件0
    if len(index) > 1:
        splitTerm1 = data[index[1]] <= 0.5*globalLimit  # 分裂条件1

    #"""决策树第一层"""
    # 如果data的第一行索引名为'ap_count',并且第一行值等于-1，那么返回‘网关离线’。（AP全掉了）
    if index[0] == 'ap_count' and data[index[0]] == -1:
        return '网关离线'

    # 如果data的第一行索引名为'ap_count',并且第一行值小于apLimi同时不等于-1，那么返回‘AP离线’。（AP掉了一部分）
    elif index[0] == 'ap_count' and data[index[0]] <= apLimit and data[index[0]] != -1:
        return 'AP离线'

    # 如果data的第一行索引名为'ap_count',并且第一行值大于apLimit，那么data去除第一行，其他参数保持不变，进行递归
    #（如果AP数量没有下降，那么进行下一步判断）
    elif index[0] == 'ap_count' and data[index[0]] > apLimit:
        return decisionTree(data[1:], rawData, apLimit, forwardLimit, globalLimit)

    #"""决策树第二层"""
    # 如果data的第一行索引名为'dhcp',并且同时满足分裂条件0和1，那么返回'未知'。（莫名其妙的DHCP数就下降了）
    elif index[0] == 'dhcp' and splitTerm0 and splitTerm1:
        return '未知  '+index[0]+'<'+str(globalLimit)

    # 如果data的第一行索引名为'dhcp',并且满足分裂条件0,不满足分裂条件1，那么返回'DHCP统计错误'。（只有DHCP数下降了）
    elif index[0] == 'dhcp' and splitTerm0 and not splitTerm1:
        return 'DHCP统计错误'

    # 如果data的第一行索引名为'dhcp',并且不满足分裂条件0，那么data去除第一行，其他参数保持不变，进行递归
    #（如果DHCP数没有下降，那么进行下一步判断）
    elif index[0] == 'dhcp' and not splitTerm0:
        return decisionTree(data[1:], rawData, apLimit, forwardLimit, globalLimit)

    #"""决策树第三层"""
    # 如果data的第一行索引名为'portal',并且同时满足分裂条件0和1，那么返回'重定向故障'。（莫名其妙的portal数就下降了）
    elif index[0] == 'portal' and splitTerm0 and splitTerm1:
        return '重定向故障'

    # 如果data的第一行索引名为'portal',并且满足分裂条件0不满足分裂条件1，那么返回'portal打点故障'。（只有portal数下降了）
    elif index[0] == 'portal' and splitTerm0 and not splitTerm1:
        return 'portal打点故障'

    # 如果data的第一行索引名为'portal',并且不满足分裂条件0，那么data去除第一行，其他参数保持不变，进行递归
    #（如果portal数没有下降，那么进行下一步判断）
    elif index[0] == 'portal' and not splitTerm0:
        return decisionTree(data[1:], rawData, apLimit, forwardLimit, globalLimit)

    #"""决策树第四层"""
    # 如果data的第一行索引名为'prelogin',并且同时满足分裂条件0和1，那么返回'带宽负载较高（疑似）'。（预登陆人数变少了，可能是带宽造成的）
    elif index[0] == 'prelogin' and splitTerm0 and splitTerm1:
        return '带宽负载较高（疑似）'

    # 如果data的第一行索引名为'prelogin',并且满足分裂条件0不满足分裂条件1，那么返回'prelogin打点故障'。（只有prelogin数下降了）
    elif index[0] == 'prelogin' and splitTerm0 and not splitTerm1:
        return 'prelogin打点故障'

    # 如果data的第一行索引名为'prelogin',并且不满足分裂条件0，那么data去除第一行，其他参数保持不变，进行递归
    #（如果prelogin数没有下降，那么进行下一步判断）
    elif index[0] == 'prelogin' and not splitTerm0:
        return decisionTree(data[1:], rawData, apLimit, forwardLimit, globalLimit)

    #"""决策树第五层"""
    # 如果data的第一行索引名为'login',并且同时满足分裂条件0和1，那么返回'未知'。（莫名其妙的login数就变少了）
    elif index[0] == 'login' and splitTerm0 and splitTerm1:
        return '未知'+index[0]+'<'+str(globalLimit)

    # 如果data的第一行索引名为'login',并且满足分裂条件0不满足分裂条件1，那么返回'login打点故障'。（只有login数下降了）
    elif index[0] == 'login' and splitTerm0 and not splitTerm1:
        return 'login打点故障'

    # 如果data的第一行索引名为'login',并且不满足分裂条件0，那么data去除第一行，其他参数保持不变，进行递归
    #（如果login数没有下降，那么进行下一步判断）
    elif index[0] == 'login' and not splitTerm0:
        return decisionTree(data[1:], rawData, apLimit, forwardLimit, globalLimit)

    #"""决策树第六层"""
    # 如果data的第一行索引名为'webforward',并且同时满足分裂条件0和1，那么返回'web放行故障'。（莫名其妙的webforward数就变少了）
    elif index[0] == 'webforward' and splitTerm0 and splitTerm1:
        return 'web放行故障'

    # 如果data的第一行索引名为'webforward',并且满足分裂条件0不满足分裂条件1，那么返回'webforward打点故障'。（只有webforward数下降了）
    elif index[0] == 'webforward' and splitTerm0 and not splitTerm1:
        return 'webforward打点故障'

    # 如果data的第一行索引名为'webforward',并且不满足分裂条件0，那么data去除第一行，其他参数保持不变，进行递归
    #（如果webforward数没有下降，那么进行下一步判断）
    elif index[0] == 'webforward' and not splitTerm0:
        return decisionTree(data[1:], rawData, apLimit, forwardLimit, globalLimit)

    #"""决策树第七层"""
    # 如果data的第一行索引名为'hardforward',并且满足分裂条件0，那么返回'硬件放行故障'。（莫名其妙的hardforward数就变少了）
    elif index[0] == 'hardforward' and splitTerm0:
        return '硬件放行故障'

    # 如果data的第一行索引名为'hardforward',并且不满足分裂条件0，那么data去除第一行，其他参数保持不变，进行递归
    #（如果hardforward数没有下降，那么进行下一步判断）
    elif index[0] == 'hardforward' and not splitTerm0:
        return decisionTree(data[1:], rawData, apLimit, forwardLimit, globalLimit)

    #"""决策树第八层"""
    # 如果data的第一行索引名为'forward_rate',并且不满足分裂条件0，那么返回'正常'。（正常情况）
    elif index[0] == 'forward_rate' and not splitTerm0:
        return '正常'

    # 如果data的第一行索引名为'forward_rate',并且满足分裂条件0，那么赋值rawData到data，globalLimit - 0.05,其他参数保持不变，进行递归
    #（如果其他参数正常，只有forward_rate变少了，那么减小分裂系数，重新进行分析，）
    elif index[0] == 'forward_rate' and not splitTerm0:
        return decisionTree(rawData, rawData, apLimit, forwardLimit, globalLimit-0.05)


def analysis(day, cycle, apLimit=-0.1, forwardLimit=-0.4, globalLimit=-0.4):
    uvDataRaw, apDataRaw, hosInfoRaw, devInfoRaw, data400Raw = getRawData(
        day, cycle)
    uvDataGood, apDataGood = dataClean(uvDataRaw, apDataRaw)
    uvDay1, uvDay2, uvDay1Lite, uvDay2Lite = getDateData(
        uvDataGood, apDataGood, day, cycle)
    rate = ((uvDay1Lite.reindex(uvDay2Lite.index).fillna(0)-uvDay2Lite)/uvDay2Lite)
    troubleList = {}
    for hos in rate.index:
        trouble = decisionTree(rate.loc[hos], rate.loc[hos], -0.1, -0.4, -0.4)
        troubleList[hos] = trouble
    statusSeries = pd.Series(troubleList)
    troubleHos = statusSeries[statusSeries != '正常']
    troubleDf = pd.DataFrame(troubleHos, columns=['故障类型'])
    troubleDf.insert(0, 'hos_id', troubleDf.index)
    troubleRate = pd.merge(troubleDf, rate, left_on='hos_id', right_index=True)
    troubleUV = pd.merge(troubleDf, uvDay1, left_on='hos_id', right_index=True)
    troubleTotal = mergeDf(troubleDf, hosInfoRaw, devInfoRaw, data400Raw)[[
        'hos_id', r'故障类型', r'AP型号', r'AP品牌', r'GWID(网关ID)', '网关品牌',
        '医院名称', '医院级别', 'status', '报修日期', '解决问题日期', '报修问题',
        '解决方式', '解决人']]
    troubleTotal[['报修日期', '解决问题日期']].astype('datetime64[ns]')
    if 0 in troubleTotal['hos_id']:
        troubleTotal = troubleTotal.drop(0)
    troubleInfo = {
        'troubleTotal': troubleTotal,
        'troubleRate': troubleRate,
        'troubleUV': troubleUV
    }
    return troubleInfo


def mergeDf(troubleDf, hosInfoRaw, devInfoRaw, data400Raw):
    hosInfoRaw.columns = hosInfoRaw.loc[0]
    hosInfoRaw = hosInfoRaw.set_index('HosID')
    hosInfoRaw = hosInfoRaw.reindex(hosInfoRaw.index.dropna()).drop('HosID')
    hosInfoRaw.index = hosInfoRaw.index.astype('int32')
    devInfo = devInfoRaw[devInfoRaw['clock'] == devInfoRaw['clock'].loc[0]]
    troubleDfTotal = pd.merge(troubleDf, hosInfoRaw,
                              left_on='hos_id', right_index=True, how='left')
    troubleDfTotal = pd.merge(
        troubleDfTotal, devInfo, left_on=r'GWID(网关ID)', right_on='gwid', how='left')
    troubleDfTotal = pd.merge(
        troubleDfTotal, data400Raw, left_on=r'医院名称', right_on='医院名称', how='left')
    return troubleDfTotal


def chineseNumber(x):
    num = ['零', '一', '二', '三', '四', '五', '六', '七', '八', '九']
    return num[x]


def englishNumber(x):
    num = ['ZERO', 'ONE', 'TWO', 'THREE', 'FOUR',
           'FIVE', 'SIX', 'SEVEN', 'EIGHT','NINE','TEN']
    return num[x]


def sendEmail(pathList, day, cycle):
    senderList = emailSender(r'/root/ipython/user_list.xlsx', r'email_sender')
    receiverList = emailReceiver(
        r'/root/ipython/user_list.xlsx', r'email_receive')
    user = senderList['qq']['user']
    pwd = senderList['qq']['password']
    to = ','.join(receiverList)
    msg = MIMEMultipart()
    title = '%s周对比分析_%s' % (chineseNumber(cycle), str(getDate(day)))
    message = '附件为%s的%s周对比分析结果' % (str(getDate(day)), chineseNumber(cycle))
    msg['Subject'] = title
#     msg['From'] = r'UV_MONITOR'
    content1 = MIMEText(message, 'plain', 'utf-8')
    msg.attach(content1)
    for path in pathList:
        attfile = path
        basename = os.path.basename(attfile)
        fp = open(attfile, 'rb')
        att = MIMEText(fp.read(), 'base64', 'utf-8')
        att["Content-Type"] = 'application/octet-stream'
        att.add_header('Content-Disposition', 'attachment',
                       filename=('gbk', '', basename))
        encoders.encode_base64(att)
        msg.attach(att)
    #-----------------------------------------------------------
    s = smtplib.SMTP('smtp.qq.com')
    s.login(user, pwd)
    s.sendmail(user, to, msg.as_string())
    s.close()


def toMySql(troubleInfo, day=1, cycle=1):
    outPutServer = getDbInfo(
        r'/root/ipython/user_list.xlsx', r'db_user')['devBase']['server']
    engine = create_engine('mysql+pymysql://%s:%s@%s:%d/%s' % (outPutServer['user'],
                                                               outPutServer[
                                                                   'password'],
                                                               outPutServer[
                                                                   'host'],
                                                               outPutServer[
                                                                    'port'],
                                                               outPutServer['db']), connect_args={'charset': 'utf8'})
    for trouble in troubleInfo:
        troubleInfo[trouble]['date'] = getDate(day)
        troubleInfo[trouble].to_sql(name='%d_TROUBLE_%s_ON_%s_WEEK' % (cycle, str.upper(trouble[7:]),
                                                                       englishNumber(cycle)),
                                    con=engine, if_exists='append', index=False)


def output(day=1, cycle=1, toEmail=True, toSql=True):
    Path = r'/root/ipython/UV分析结果/%s周对比/%s周对比_%s.xlsx' % (
        chineseNumber(cycle), chineseNumber(cycle), str(getDate(day)))
    day2 = cycle*7
    writer = pd.ExcelWriter(Path)
    troubleInfo = analysis(day, day2)
    pathList = [Path]
    for x in troubleInfo:
        troubleInfo[x].to_excel(writer, x)
    writer.save()
    if toEmail:
        sendEmail(pathList, day, cycle)
    if toSql:
        toMySql(troubleInfo, day, cycle)


def main():
    output(1, 1)
    output(1, 4, toEmail=False)

if __name__ == '__main__':
    main()
