import pymysql as psql
from contextlib import contextmanager
import datetime
import pandas as pd
# 使用上下文管理器来连接数据库的函数


@contextmanager
def connect(server):
    database = psql.connect(**server)
    yield database
    database.close()


def sqlAction(database, action):
    with database.cursor() as cursor:
        # 执行sql语句，进行查询
        cursor.execute(action)
        # 获取查询结果
        result = cursor.fetchall()
    # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
    database.commit()
    return result

# 获取x天之前的日期

def chineseNumber(x):
    num = ['零', '一', '二', '三', '四', '五', '六', '七', '八', '九','十']
    return num[x]


def englishNumber(x):
    num = ['ZERO', 'ONE', 'TWO', 'THREE', 'FOUR',
           'FIVE', 'SIX', 'SEVEN', 'EIGHT','NINE','TEN']
    return num[x]


def getDate(x):
    today = datetime.date.today()
    oneday = datetime.timedelta(days=x)
    day = today-oneday
    return day

# 定义sqlServer类，用来连接到mysql获取数据
class SqlServer(object):  
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
#         self.sqlComm = server["comm"]

#     只执行一条命令
    def getOneData(self, comm):
        server = self.server
        with connect(server) as database:
            result = sqlAction(database, comm)
#             print(result)
        try:
            df = pd.DataFrame(result)
        except:
            df = pd.DataFrame([])
        return df

# #     执行所有命令
#     def getAllData(self):
#         server = self.server
#         sqlDatas = {}
#         with connect(server) as database:
#             for key in self.sqlComm:
#                 result = pd.DataFrame(sqlAction(database, self.sqlComm[key]))
#                 sqlDatas[key] = result
#         return sqlDatas