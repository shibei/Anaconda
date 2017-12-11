from public_class import *
from info import *
import json

class RawData(object):
    """docstring for rawData"""

    def __init__(self, day1 = 1, day2 = 8,day0 = 0,
                 infoPath=r'/root/ipython/user_list.xlsx',
                 infoSheet=r'db_user'):
        # super(rawData, self).__init__()
        self.day1 = day1
        self.day2 = day2
        self.day0 = day0
        self.infoPath = infoPath
        self.infoSheet = infoSheet
        self.dbList = getDbInfo(infoPath, infoSheet)


    # __dbList__ = getDbInfo(self.infoPath, self.infoSheet)

    def getUvData(self):
        day1 = str(getDate(self.day1))
        day2 = str(getDate(self.day2))
        uvBase = self.dbList['uvBase']
        uvDatabase = SqlServer(uvBase)
        uvDataComm = "SELECT * FROM `funnel_model2` WHERE `log_date` = '%s' \
        OR `log_date` = '%s' ORDER BY `log_date` DESC" % (day2, day1)
        columns = ['hos_id', 'dhcp', 'portal', 'prelogin',
                   'login', 'webforward', 'hardforward', 'log_date']
        uvDataRaw = uvDatabase.getOneData(uvDataComm)
        for x in range(len(columns)):
            p = uvDataRaw.pop(columns[x])
            uvDataRaw.insert(x, columns[x], p)
        return uvDataRaw

    def getApData(self):
        day1 = str(getDate(self.day1))
        day2 = str(getDate(self.day2))
        uvBase = self.dbList['uvBase']
        uvDatabase = SqlServer(uvBase)
        apDataComm = "SELECT * FROM `ap_count` WHERE `log_date` = '%s' OR \
        `log_date` = '%s' ORDER BY `log_date` DESC" % (day2, day1)
        uvDataRaw = uvDatabase.getOneData(apDataComm)
        return uvDataRaw

    def get400Data(self, path=r'/var/400data/400.xlsx'):
        day1 = str(getDate(self.day1))
        day2 = str(getDate(self.day2))
        data400 = pd.read_excel(path)
        data400.columns = data400.loc[0].values.tolist()
        data400 = data400.drop(0)
        data4001 = pd.DataFrame([])
        data4001[[r'报修日期', r'解决问题日期']] = data400[
            [r'报修日期', r'解决问题日期']].dropna().astype('datetime64[ns]')
        data4001[[r'医院名称', r'报修问题', r'解决方式', r'解决人']] = data400[
            [r'医院名称', r'报修问题', r'解决方式', r'解决人']]
        return data4001[(data4001[r'报修日期'] >= day2) & (data4001[r'报修日期'] <= day1)]

    def getHosInfo(self):
        day1 = str(getDate(self.day1))
        day2 = str(getDate(self.day2))
        totalInfoFields = json.dumps(['HosID', 'Hospital', 'Value', 'CurrentBroadbandBandwidth',
                                      'GatewayVendor', 'ApVendor', 'ApType',
                                      'GWID']).strip('[').strip(']').replace('"', '')
        hosBaseComm = "SELECT %s FROM `TOTAL_INFO_T_%s`" % (
            totalInfoFields, day1.replace('-', ''))
        hosBase = self.dbList['hosBase']
        hosDatabase = SqlServer(hosBase)
        hosInfoRaw = hosDatabase.getOneData(hosBaseComm)
        return hosInfoRaw

    def getDevInfo(self):
        day0 = str(getDate(self.day0))
        devBase = self.dbList['devBase']
        devInfoFields = json.dumps(['clock', 'cpuUsed', 'downSpeed', 'gwid',
                                    'starttimeLong', 'status', 'wanIp']).strip('[').strip(']').replace('"', '')
        devBaseComm = "SELECT %s FROM `dev_info` WHERE `date` = '%s' ORDER BY `clock` DESC LIMIT 0, 1000" % (
            devInfoFields, day0)
        devDatabase = SqlServer(devBase)
        devInfoRaw = devDatabase.getOneData(devBaseComm)
        return devInfoRaw
