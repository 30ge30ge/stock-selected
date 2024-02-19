import tushare as ts
import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime
# 此脚本监控北向资金
import akshare as ak


class Getdata():
    def __init__(self,dec,jan,today):
        # 时间
        self.dec = dec
        self.jan= jan
        self.today = today
        self.pro  = ts.pro_api('your_token')
        #初始化行业匹配表
        self.hyppb =pd.read_excel('行业匹配表.xlsx')

    # 获取北向数据akshare
    def northfound(self, dec,jan,today):
        #获取交易日历
        df_date = self.pro.trade_cal(exchange='', start_date='20231201', end_date=today)
        df_date =df_date.sort_values(by='cal_date')
        df_date =df_date[df_date['cal_date']<=today]
        df_date =df_date[df_date['is_open']==1].tail(2)
        #交易日历筛选排序
        date_list =[dec,jan] + df_date['cal_date'].tolist()
        print(date_list)
        hyppb = self.hyppb
        # vol 持股数量(股)
        # ratio 持股占比（%），占已发行股份百分比
        # 获取单日全部持股
        for i in tqdm(date_list):
            print('获取{}日期数据'.format(i))
            try:
                df = self.pro.hk_hold(trade_date=i)
                df = df[['ts_code', 'vol','ratio']]
                df['vol'] = round(df['vol'] / 10000, 2)
                df.columns = ['证券代码', i + '持股数量(万股)', i + '持股占比(%)']
                hyppb = pd.merge(hyppb, df, on='证券代码', how='left')
            except:
                continue
        return hyppb






if __name__ == '__main__':
    dec='20231229'
    jan='20240131'
    #修改today的日期，其他不调整
    yesterday='20240201'
    today='20240202'
    specified_date = datetime.datetime.strptime(today, '%Y%m%d')

    data = Getdata(dec,jan,today)
    northdate=data.northfound(dec,jan,today)
    northdate['1月环比']= (northdate['20240131持股数量(万股)']-northdate['20231229持股数量(万股)'])/northdate['20231229持股数量(万股)']
    northdate['1月环比']=round(northdate['1月环比'],2)
    northdate['日月环比'] = (northdate[today+'持股数量(万股)'] - northdate['20240131持股数量(万股)']) / northdate[
        '20240131持股数量(万股)']
    northdate['日月环比'] = round(northdate['日月环比'], 2)
    northdate['今日环比'] = (northdate[today + '持股数量(万股)'] - northdate[yesterday+'持股数量(万股)']) / northdate[
        yesterday+'持股数量(万股)']
    northdate['今日环比'] = round(northdate['今日环比'], 2)
    print(northdate,northdate.columns)
    northdate.to_excel(today + '_北向数据监控.xlsx', index=False)






