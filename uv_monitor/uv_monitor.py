from raw_data import RawData
from pre_process import DataClean as dc
from pre_process import DateData as dd
from data_analysis import Analysis
from data_merge import Merge
from storage import Storage
from report import Report
import numpy as np
from email_data import Mail


def uvMonitor(day = 1,cycle = 1,sql = True,excel = True ,report = True,mail = True):
    day1 = day
    cycle = cycle
    day2 = day1 + cycle*7
    
    #获取原始数据
    rawData_ = RawData(day1=day1, day2=day2)
    uvData = rawData_.getUvData()
    apData = rawData_.getApData()
    data400 = rawData_.get400Data()
    devInfo = rawData_.getDevInfo()
    hosInfo = rawData_.getHosInfo()

    #预处理
    cleanData = dc(uvData, apData)
    uvDataGood, apDataGood = cleanData.uvDataClean(), cleanData.apDataClean()
    dataData = dd(uvDataGood, apDataGood, day1=day1, day2=day2)
    uvDay1, uvDay2 = dataData.uvDay1Lite, dataData.uvDay2Lite

    #对比分析
    analData = Analysis(uvDay1, uvDay2)
    trouble, rate = analData.comparison()

    #数据合并
    mergeData = Merge(troubleHos=trouble, uvDay1=uvDay1, uvDay2=uvDay2,
                      rate=rate, data400=data400, devInfo=devInfo, hosInfo=hosInfo)
    dictData = mergeData.allData()

    #存储
    saveData = Storage(data=dictData, day=day1, cycle=cycle)
    saveData.toMySql()
    excelPath = saveData.toExcel()

    #生成报告
    reportData = Report(data=dictData, day=day1, cycle=cycle)
    reportPath = reportData.makeReport()

    #发送邮件
    pathList = [excelPath, reportPath]
    mailData = Mail(pathList=pathList, day=day1, cycle=cycle)
    mailData.sendEmail()
    
def main():
    uvMonitor()

    
if __name__ == '__main__':
    main()
