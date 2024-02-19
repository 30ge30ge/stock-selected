import tushare as ts
import numpy as np
import pandas as pd
from tqdm import tqdm

class Getdata():
    def __init__(self,last_date,start_date,end_date,date):
        # 时间
        self.last_date = last_date
        self.start_date = start_date
        self.end_date = end_date
        self.pro  = ts.pro_api('your_token')

        # 初始化聪明资金
        self.smart_investors = [
            '澳门金融管理局', 'UBS AG', 'JPMORGAN', '香港中央结算有限公司', '全国社保基金',
            '阿布达比投资局', '魁北克储蓄投资集团', 'MORGAN STANLEY', '挪威中央银行',
            '新加坡华侨银行有限公司', '葛卫东', '陈发树', '上海重阳战略投资有限公司', '汇添富基金管理股份有限公司',
            '上海高毅资产管理合伙企业',
            '基本养老保险基金', '广发基金管理有限公司', '法国巴黎银行', '安本标准投资管理',
            '大成基金管理有限公司', '香港金融管理局', '科威特政府投资局'
        ]
        # 模糊匹配阈值
        self.fuzzy_threshold = 90
        self.date = date
        #初始化行业匹配表
        self.hyppb =pd.read_excel('行业匹配表.xlsx')


    def getyjbb(self,last_date,start_date,end_date):
        # q_gr_yoy:营业总收入同比增长率(%)(单季度)
        # q_netprofit_yoy:归属母公司股东的净利润同比增长率(%)(单季度)
        # q_netprofit_qoq:归属母公司股东的净利润环比增长率(%)(单季度)
        # q_gr_qoq :营业总收入环比增长率(%)(单季度)
        # roe:净资产收益率
        # q_gsprofit_margin：销售毛利率(单季度)
        # q_profit_to_gr 净利润／营业总收入(单季度)
        # dt_netprofit_yoy:归属母公司股东的净利润 - 扣除非经常损益同比增长率(%)
        # extra_item:非经常性损益
        # profit_dedt:扣除非经常性损益后的净利润
        # q_dtprofit :扣除非经常损益后的单季度净利润

        #取去年数据做单季度的扣非净利润同比
        df_lastdatedata=self.pro.fina_indicator_vip(period=last_date,fields='ts_code,q_dtprofit')
        df_lastdatedata.columns=['ts_code','扣非净利润(去年同期)']
        #取上季度的数据
        df_predata = self.pro.fina_indicator_vip(period=start_date,fields='ts_code,q_netprofit_yoy,profit_dedt,extra_item')
        # 计算归母净利润
        df_predata['pre_profit'] = df_predata['profit_dedt'] + df_predata['extra_item']
        print(df_predata.columns)
        df_predata = df_predata[['ts_code','pre_profit','q_netprofit_yoy']]
        # 把表名修改
        df_predata.columns = ['ts_code', 'pre_profit', '净利润同比(上季单季度)']
        # 取本季度的数据
        df_data = self.pro.fina_indicator_vip(period=end_date,fields='ts_code,q_netprofit_qoq,ann_date,'
                                                                     'q_netprofit_yoy,q_gr_yoy,q_gr_qoq,q_profit_to_gr,'
                                                                     'q_roe,q_gsprofit_margin,dt_netprofit_yoy,profit_dedt,extra_item,q_dtprofit')
        # 计算归母净利润
        df_data['profit']=df_data['profit_dedt']+df_data['extra_item']

        data_bjd_merge = pd.merge(df_data, df_predata, on='ts_code',how='left')
        # 求当季净利润
        data_bjd_merge['净利润(本季)'] = round((data_bjd_merge['profit'] - data_bjd_merge['pre_profit']) / 100000000, 2)
        data_bjd_merge['净利润(上季)'] = round(data_bjd_merge['净利润(本季)'] / (data_bjd_merge['q_netprofit_qoq'] / 100 + 1), 2)


        # 求单季度扣非净利润同比增长
        data_bjd_merge=pd.merge(data_bjd_merge,df_lastdatedata,on='ts_code',how='left')
        data_bjd_merge['扣非净利润同比(本季单季度)'] = (data_bjd_merge['q_dtprofit'] - data_bjd_merge['扣非净利润(去年同期)'])/abs(data_bjd_merge['扣非净利润(去年同期)'])*100
        data_bjd_merge['扣非净利润同比(本季单季度)'] = round(data_bjd_merge['扣非净利润同比(本季单季度)'], 2)
        #取想要的列
        data_bjd_merge =data_bjd_merge[['ts_code','净利润(上季)','净利润(本季)','q_netprofit_qoq',
                                        '净利润同比(上季单季度)','q_netprofit_yoy','q_roe',
                                        'q_gr_yoy','q_gr_qoq','q_gsprofit_margin',
                                        'q_profit_to_gr','扣非净利润同比(本季单季度)','ann_date']]

        cols=['证券代码','净利润(上季)','净利润(本季)','净利润环比增长',
              '净利润同比(上季单季度)','净利润同比(本季单季度)','净资产收益率(本季单季度)',
              '营业收入同比增长(本季单季度)','营业收入环比增长(本季单季度)','销售毛利率(单季度)',
              '净利润率(本季单季度)','扣非净利润同比(本季单季度)','最新公告日期']
        # 变成列
        data_bjd_merge.columns=cols

        # 和初始化行业匹配 表合并
        data_all= pd.merge(self.hyppb,data_bjd_merge,on='证券代码',how='left')
        # 去重
        data_all = data_all.drop_duplicates(subset='证券代码', keep='last')
        # 排序
        data_all = data_all.sort_values(by=['一级行业', '二级行业', '三级行业'])

        # 选择第8到第16列并应用去极值函数
        # data_all.iloc[:, 7:15] = data_all.iloc[:, 7:15].apply(lambda x: self.quantile(x, 90, 10))
        # 选择第8到第16列保留2位小数点
        # data_all.iloc[:, 7:15] = data_all.iloc[:, 7:15].apply(lambda x: x.round(0))
        # 重制索引
        data_all = data_all.reset_index(drop=True)
        print(data_all)

        data_all.to_excel(self.end_date+'_业绩报告(单季度).xlsx',index=False)

    #定义去极值函数
    def quantile(self,factor, up, down):
        """分位数去极值，保留空值不受影响"""
        # 使用0填充空值
        factor = factor.fillna(0)
        if len(factor) > 0:
            up_scale = np.percentile(factor, up)
            down_scale = np.percentile(factor, down)
            factor = np.where(factor > up_scale, up_scale, factor)
            factor = np.where(factor < down_scale, down_scale, factor)
        return factor




if __name__ == '__main__':
    # 去年同期时间
    last_date = '20221231'
    # 起始日期
    start_date ='20230930'
    # 截止日期
    end_date ='20231231'
    # 特定时间日期,取特定时间的数据
    date='20231031'

    data = Getdata(last_date,start_date,end_date,date)
    datamerge = data.getyjbb(last_date,start_date,end_date)




