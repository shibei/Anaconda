import pandas as pd


class Analysis(object):
    """docstring for Analysis"""

    def __init__(self, uvDay1, uvDay2, apLimit=-0.1, forwardLimit=-0.4, globalLimit=-0.4):
        # super(Analysis, self).__init__()
        self.uvDay1 = uvDay1
        self.uvDay2 = uvDay2
        self.apLimit = apLimit
        self.forwardLimit = forwardLimit
        self.globalLimit = globalLimit

    # 依靠经验值来构建的决策树
    def _decisionTree(self, data, rawData, apLimit=-0.4, forwardLimit=-0.4, globalLimit=-0.4):
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
            return self._decisionTree(data[1:], rawData, apLimit, forwardLimit, globalLimit)

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
            return self._decisionTree(data[1:], rawData, apLimit, forwardLimit, globalLimit)

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
            return self._decisionTree(data[1:], rawData, apLimit, forwardLimit, globalLimit)

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
            return self._decisionTree(data[1:], rawData, apLimit, forwardLimit, globalLimit)

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
            return self._decisionTree(data[1:], rawData, apLimit, forwardLimit, globalLimit)

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
            return self._decisionTree(data[1:], rawData, apLimit, forwardLimit, globalLimit)

        #"""决策树第七层"""
        # 如果data的第一行索引名为'hardforward',并且满足分裂条件0，那么返回'硬件放行故障'。（莫名其妙的hardforward数就变少了）
        elif index[0] == 'hardforward' and splitTerm0:
            return '硬件放行故障'

        # 如果data的第一行索引名为'hardforward',并且不满足分裂条件0，那么data去除第一行，其他参数保持不变，进行递归
        #（如果hardforward数没有下降，那么进行下一步判断）
        elif index[0] == 'hardforward' and not splitTerm0:
            return self._decisionTree(data[1:], rawData, apLimit, forwardLimit, globalLimit)

        #"""决策树第八层"""
        # 如果data的第一行索引名为'forward_rate',并且不满足分裂条件0，那么返回'正常'。（正常情况）
        elif index[0] == 'forward_rate' and not splitTerm0:
            return '正常'

        # 如果data的第一行索引名为'forward_rate',并且满足分裂条件0，那么赋值rawData到data，globalLimit - 0.05,其他参数保持不变，进行递归
        #（如果其他参数正常，只有forward_rate变少了，那么减小分裂系数，重新进行分析，）
        elif index[0] == 'forward_rate' and not splitTerm0:
            return self._decisionTree(rawData, rawData, apLimit, forwardLimit, globalLimit-0.05)

    def comparison(self):
        rate = ((self.uvDay1.reindex(self.uvDay2.index).fillna(
            0)-self.uvDay2)/self.uvDay2)
#         print(rate.index)
        troubleList = {}
        for hos in rate.index:
            #             print(rate.loc[hos])
            data = rate.loc[hos]
            trouble = self._decisionTree(data=data, rawData=data, apLimit=self.apLimit,
                                         forwardLimit=self.forwardLimit, globalLimit=self.globalLimit)
            troubleList[hos] = trouble
        statusSeries = pd.Series(troubleList)
        troubleHos = statusSeries[statusSeries != '正常']
        troubleDf = pd.DataFrame(troubleHos, columns=['故障类型'])
        troubleDf.insert(0, 'hos_id', troubleDf.index)
#         troubleRate = pd.merge(troubleDf, rate, left_on='hos_id', right_index=True)
        if 0 in troubleDf.index :
            troubleDf = troubleDf.drop(0)
        return troubleDf,rate
