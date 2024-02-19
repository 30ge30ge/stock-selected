import tushare as ts
import numpy as np
import pandas as pd
from tqdm import tqdm
import akshare as ak

class Getdata():
    def __init__(self,last_date,start_date,end_date,date):
        # 时间
        self.last_date = last_date
        self.start_date = start_date
        self.end_date = end_date
        self.pro  = ts.pro_api('your_token')
        self.date = date
        #初始化行业匹配表
        self.hyppb =pd.read_excel('行业匹配表.xlsx')


    def getyjyg(self,pre_date,start_date,end_date):
        #先把上一季度的净利润计算出来
        # 取上2季度的数据
        df_predata = self.pro.fina_indicator_vip(period=pre_date,
                                                 fields='ts_code,q_netprofit_yoy,profit_dedt,extra_item')
        # 计算归母净利润
        df_predata['pre_profit'] = df_predata['profit_dedt'] + df_predata['extra_item']
        print(df_predata.columns)
        df_predata = df_predata[['ts_code', 'pre_profit', 'q_netprofit_yoy']]
        # 把表名修改
        df_predata.columns = ['ts_code', 'pre_profit', '净利润同比(上季单季度)']
        # 取上季度的数据
        df_data = self.pro.fina_indicator_vip(period=start_date, fields='ts_code,q_netprofit_qoq,ann_date,'
                                                                      'q_netprofit_yoy,q_gr_yoy,q_gr_qoq,q_profit_to_gr,'
                                                                      'q_roe,q_gsprofit_margin,dt_netprofit_yoy,profit_dedt,extra_item,q_dtprofit')
        # 计算归母净利润
        df_data['profit'] = df_data['profit_dedt'] + df_data['extra_item']

        data_sjd_merge = pd.merge(df_data, df_predata, on='ts_code', how='left')
        # 求上季净利润
        data_sjd_merge['净利润(上季单季度)'] = round((data_sjd_merge['profit'] - data_sjd_merge['pre_profit']) / 100000000, 2)
        print(data_sjd_merge)
        #保留上季度的净利润数据
        data_sjd_merge=data_sjd_merge[['ts_code','净利润(上季单季度)','profit']]

        #取本季度预告
        df_yjyg =self.pro.forecast_vip(period=end_date,
                     fields='ts_code,ann_date,end_date,p_change_min,p_change_max,net_profit_min,net_profit_max')

        df_yjyg['证券代码']= df_yjyg['ts_code']
        df_yjyg['公告日期']=df_yjyg['ann_date']
        # 和上季度数据表合并
        df_yjyg= pd.merge(data_sjd_merge,df_yjyg,on='ts_code',how='right')

        df_yjyg['累积净利润']=round((df_yjyg['net_profit_min']+df_yjyg['net_profit_max'])/20000,2)
        df_yjyg['净利润同比'] =round((df_yjyg['p_change_min']+df_yjyg['p_change_max'])/2,2)
        df_yjyg['净利润(本季单季度)'] =round(df_yjyg['累积净利润']-df_yjyg['profit']/100000000,2)
        print(df_yjyg)
        df_yjyg['净利润环比'] =round((df_yjyg['净利润(本季单季度)']-df_yjyg['净利润(上季单季度)'])/abs(df_yjyg['净利润(上季单季度)'])*100,2)
        df_yjyg=df_yjyg[['证券代码','净利润同比','净利润环比','累积净利润','净利润(本季单季度)','净利润(上季单季度)','公告日期']]


        stock_yjyg_em_df = df_yjyg.drop_duplicates(subset='证券代码', keep='first')
        # 和初始化行业匹配 表合并
        data_all= pd.merge(self.hyppb,stock_yjyg_em_df,on='证券代码',how='right')
        # 去重
        data_all = data_all.drop_duplicates(subset='证券代码', keep='first')
        # 排序
        data_all = data_all.sort_values(by=['一级行业', '二级行业', '三级行业'])
        #

        data_all=data_all.dropna()
        data_all.replace([np.inf, -np.inf], np.nan, inplace=True)
        data_all=data_all.reset_index(drop=True)



        stock_yjkb_em_df = ak.stock_yjkb_em(date=end_date)
        stock_yjkb_em_df['证券代码'] = stock_yjkb_em_df['股票代码'].apply(
            lambda X: X[:6] + '.SH' if X[0] == "6" else X[:6] + '.SZ')
        data_nb = pd.merge(self.hyppb, stock_yjkb_em_df, on='证券代码', how='right')
        print(data_nb)
        data_nb = data_nb[['一级行业','二级行业','三级行业','证券代码','证券名称','序号',
                           '每股收益','营业收入-同比增长','营业收入-季度环比增长','净利润-净利润',
                           '净利润-同比增长','净利润-季度环比增长','净资产收益率','公告日期']]

        # 逐列保留小数点
        data_nb['净利润-净利润']=data_nb['净利润-净利润']/100000000
        for column in data_nb.columns[6:13]:
            data_nb[column] = round(data_nb[column],2)

        data_all.to_excel(self.date + '_业绩预告.xlsx', sheet_name='业绩预告',index=False)
        # 写入 data_nb，作为第二个 sheet
        with pd.ExcelWriter(self.date + '_业绩预告.xlsx', engine='openpyxl', mode='a') as writer:
            data_nb.to_excel(writer, sheet_name='业绩快报', index=False)





if __name__ == '__main__':
    # 去年同期时间
    pre_date = '20230630'
    # 起始日期
    start_date ='20230930'
    # 截止日期
    end_date ='20231231'
    # 特定时间日期,取特定时间的数据
    date='20240208'

    data = Getdata(pre_date,start_date,end_date,date)
    datamerge = data.getyjyg(pre_date,start_date,end_date)




