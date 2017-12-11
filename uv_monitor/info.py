import pandas as pd
import numpy as np
import pymysql.cursors

# 从文件中获得DB信息
def getDbInfo(file, sheet):
    serverDf = np.transpose(pd.read_excel(file, sheet))
    serverCommList = {}
    for server in serverDf:
        serverComm = {}
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
