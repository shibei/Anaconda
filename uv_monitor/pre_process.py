import public_class as public
import numpy as np
import pandas as pd
# 清洗数据


class DataClean(object):
    """docstring for DataClean"""

    def __init__(self, uvData, apData):
        # super(DataClean, self).__init__()
        self.uvData = uvData
        self.apData = apData
    # 清洗UV数据：替换源数据中的"\N"、更改数据type，构建新特征"forward_rate"

    def uvDataClean(self):
        uvData = self.uvData
        # 判断源数据集是否为空，如果为空数据集则不做处理直接输出
        if len(uvData) > 0:
            # 把源数据集中的"\N"符号替换为0
            uvData = uvData.replace('\\N', 0).fillna(0).sort_values(
                by='log_date', ascending=False).reset_index(drop=True)
            # 将源数据集中的'hos_id', 'dhcp', 'portal', 'prelogin','login',
            # 'webforward', 'hardforward'字段的类型更改为"int32"
            uvDataGood = uvData[['hos_id', 'dhcp', 'portal', 'prelogin',
                                 'login', 'webforward', 'hardforward']].astype('int32')
            # 将源数据集中的"log_date"字段类型更改为"datetime64[ns]"
            uvDataGood['log_date'] = uvData[
                'log_date'].astype('datetime64[ns]')
            # 构建新特征"forward_rate"
            uvDataGood['forward_rate'] = uvDataGood[
                'hardforward']/uvDataGood['dhcp']
            # 备选输出："uvDataLite"。只包含'portal', 'prelogin', 'login',
            # 'webforward'字段
            uvDataLite = uvDataGood.drop(
                ['portal', 'prelogin', 'login', 'webforward'], axis=1)
        else:
            uvDataGood = uvData
        return uvDataGood
    # 清洗AP数据：替换特殊字符、更改数据type

    def apDataClean(self):
        apData = self.apData
        if len(apData) > 0:
            # 替换特殊字符
            apData = apData.replace(
                ['福建', '安徽', '福建省', '安徽省', ''], np.nan).dropna()
            # 更改数据type
            apDataGood = apData[['hos_id', 'ap_count']].astype('int64')
            apDataGood['log_date'] = apData[
                'log_date'].astype('datetime64[ns]')
        else:
            apDataGood = apData
        return apDataGood

# 提取day1天和day2的数据


class DateData(object):
    """docstring for getDateData"""

    def __init__(self, uvData, apData, day1=1, day2=8):
        # super(getDateData, self).__init__()
        self.uvData = uvData
        self.apData = apData
        self.day1 = str(public.getDate(day1))
        self.day2 = str(public.getDate(day2))
        # 中间数据
        self._uvDataDay1 = self._getDateData(self.day1)
        self._uvDataDay2 = self._getDateData(self.day2)
        # hosid的并集
        self._allHosid = self._getAllHosid()

        self.uvDay1 = self._reindexData(self._uvDataDay1)
        self.uvDay2 = self._reindexData(self._uvDataDay2)
        self.uvDay1Lite, self.uvDay2Lite = self._dataLite()
    # 获取day天的uv数据和ap数据并进行合并。输出合并后的数据uvDay

    def _getDateData(self, day):
        # print(day)
        uvData = self.uvData
        apData = self.apData
        # day = self.day2
        if len(uvData) > 0:
            uvDay = uvData[uvData['log_date'] == day].reset_index(
                drop=True).set_index('hos_id').drop('log_date', axis=1)

        if len(apData) > 0:
            apDay = apData[apData['log_date'] == day].set_index(
                'hos_id').drop('log_date', axis=1)

        uvDay = pd.merge(uvDay, apDay, left_index=True,
                         right_index=True, how='left')
        return uvDay
    # 获取day1和day2数据hosid的并集

    def _getAllHosid(self):
        return np.union1d(self._uvDataDay1.index, self._uvDataDay2.index)
    # 用hosid的并集对数据重新索引

    def _reindexData(self, data):
        dataReindex = data.reindex(self._allHosid, fill_value=0)
        if len(self.apData) > 0:
            dataReindex.insert(0, 'ap_count', dataReindex.pop('ap_count'))
        return dataReindex
    # 肢体去dhcp大于200数据

    def _dataLite(self):
        uvDay2 = self.uvDay2
        uvDay1 = self.uvDay1
        uvDay2Lite = uvDay2[uvDay2['dhcp'] > 200]
        uvDay1Lite = uvDay1.reindex(uvDay2Lite.index)
        return uvDay1Lite, uvDay2Lite
