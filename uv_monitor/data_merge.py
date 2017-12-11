import pandas as pd
import numpy as np

class Merge(object):
    """docstring for Merge"""

    def __init__(self, troubleHos, rate, uvDay1, uvDay2, hosInfo, devInfo, data400):
        # super(Merge, self).__init__()
        self.troubleHos = troubleHos
        self.rate = rate
        self.uvDay1 = uvDay1
        self.uvDay2 = uvDay2
        self.hosInfo = hosInfo
        self.devInfo = devInfo
        self.data400 = data400

    def _mergeDf(self):
        troubleDf = self.troubleHos
        hosInfoRaw = self.hosInfo
        devInfoRaw = self.devInfo
        data400Raw = self.data400

        hosInfoRaw.columns = hosInfoRaw.loc[0]
        hosInfoRaw = hosInfoRaw.set_index('HosID')
        hosInfoRaw = hosInfoRaw.reindex(
            hosInfoRaw.index.dropna()).drop('HosID')
        hosInfoRaw.index = hosInfoRaw.index.astype('int32')
        devInfo = devInfoRaw[devInfoRaw['clock'] == devInfoRaw['clock'].loc[0]]
        troubleDfTotal = pd.merge(troubleDf, hosInfoRaw,
                                  left_on='hos_id', right_index=True, how='left')
        troubleDfTotal = pd.merge(
            troubleDfTotal, devInfo, left_on=r'GWID(网关ID)', right_on='gwid', how='left')
        troubleDfTotal = pd.merge(
            troubleDfTotal, data400Raw, left_on=r'医院名称', right_on='医院名称', how='left')
        return troubleDfTotal

    def mergeRate(self):
        return pd.merge(self.troubleHos, self.rate, left_on='hos_id',right_index=True).set_index('hos_id').replace([np.inf,-np.inf],np.nan)

    def mergeUvDay1(self):
        return pd.merge(self.troubleHos, self.uvDay1, left_on='hos_id', right_index=True).set_index('hos_id')

    def mergeUvDay2(self):
        return pd.merge(self.troubleHos, self.uvDay2, left_on='hos_id', right_index=True).set_index('hos_id')

    def mergeInfo(self):
        troubleTotal = self._mergeDf()[[
            'hos_id', r'故障类型', r'AP型号', r'AP品牌', r'GWID(网关ID)', '网关品牌',
            '医院名称', '医院级别', 'status', '报修日期', '解决问题日期', '报修问题',
            '解决方式', '解决人']].drop_duplicates('hos_id')
        troubleTotal[['报修日期', '解决问题日期']].astype('datetime64[ns]')
        if 0 in troubleTotal['hos_id']:
            troubleTotal = troubleTotal.drop(0)
        return troubleTotal.set_index('hos_id')

    def allData(self):
        allData = {
            'troubleTotal': self.mergeInfo(),
            'troubleRate': self.mergeRate(),
            'troubleUvDay1': self. mergeUvDay1(),
            'troubleUvDay2':self.mergeUvDay2()
        }
        return allData
    
