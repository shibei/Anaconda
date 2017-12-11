import public_class as public
import pandas as pd
import info
import os
from sqlalchemy import create_engine


class Storage(object):
    """docstring for Storage"""

    def __init__(self, data, day, cycle, path=r'/root/ipython/UV分析结果/'):
        # super(Storage, self).__init__()
        self.data = data
        self.day = day
        self.cycle = cycle
        self.path = path

    def toExcel(self):
        path = self.path
        troubleInfo = self.data
        weekFolder = path + '%s周对比/' % public.chineseNumber(self.cycle)
        fileName = '%s周对比_%s.xlsx' % (public.chineseNumber(
            self.cycle), str(public.getDate(self.day)))
        path = weekFolder + fileName

        if not os.path.exists(weekFolder):
            os.makedirs(weekFolder)

        writer = pd.ExcelWriter(path)
        for x in troubleInfo:
            troubleInfo[x].to_excel(writer, x)
        writer.save()
        return path

    def toMySql(self):
        troubleInfo = self.data
        outPutServer = info.getDbInfo(r'/root/ipython/user_list.xlsx',
         r'db_user')['devBase']['server']
        engine = create_engine('mysql+pymysql://%s:%s@%s:%d/%s' % (outPutServer['user'], 
            outPutServer['password'], outPutServer['host'], outPutServer['port'], 
            outPutServer['db']), connect_args={'charset': 'utf8'})
        for trouble in troubleInfo:
            troubleInfo[trouble]['date'] = public.getDate(self.day)
            try:
                troubleInfo[trouble].to_sql(name='%d_TROUBLE_%s_ON_%s_WEEK' % (self.cycle, 
                    str.upper(trouble[7:]),public.englishNumber(self.cycle)),con=engine, 
                    if_exists='append', index=True)
            except:
                pass
            troubleInfo[trouble].pop('date')
            
