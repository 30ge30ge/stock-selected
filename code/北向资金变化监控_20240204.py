import pandas as pd
import requests
import json
import tushare as ts
import numpy as np
from tqdm import tqdm
import akshare as ak




class Getdata():
    def __init__(self,date):
        self.df = pd.read_excel('行业匹配表.xlsx')
        self.date =date
        self.pro = ts.pro_api('your_token')
        self.dates_str =None


    def northfound(self):
        # 获取交易日历
        df_date = self.pro.hk_tradecal(exchange='', start_date='20231001', end_date=self.date)
        df_date = df_date.sort_values(by='cal_date')
        df_date = df_date[df_date['cal_date'] <= self.date]
        df_date = df_date[df_date['is_open'] == 1]
        # 将 'cal_date' 列转换为 datetime 类型
        df_date['cal_date'] = pd.to_datetime(df_date['cal_date'])
        # 找到每个月的最后一天
        last_day_of_month = df_date.resample('M', on='cal_date')['cal_date'].max().tolist()
        dates_str = [date.strftime('%Y%m%d') for date in last_day_of_month]
        self.dates_str =dates_str

        merged_df = pd.DataFrame()

        for i in tqdm(dates_str):
            print('获取{}日期数据'.format(i))
            try:
                if i ==self.date:
                    df = self.pro.hk_hold(trade_date=i)
                    df['vol'] = round(df['vol'] / 10000, 2)
                    df = df[['ts_code', 'vol','ratio']]
                    df.columns = ['证券代码', i + '持股数量(万股)',i + '持股占比(%)']
                else:
                    df=self.pro.hk_hold(trade_date=i)
                    df['vol'] =round(df['vol'] /10000,2)
                    df=df[['ts_code', 'vol']]
                    # df = df[['股票代码', '持股数量']]
                    df.columns = ['证券代码', i + '持股数量(万股)']
                # df['证券代码'] = df['证券代码'].apply(lambda X: X[:6] + '.SH' if X[0] == "6" else X[:6] + '.SZ')
                if not merged_df.empty:
                    merged_df = pd.merge(merged_df, df, on='证券代码', how='outer')
                else:
                    merged_df = df.copy()
            except Exception as e:
                print(f'获取{dates_str}日期数据出错：{e}')
                continue
        # print('最终结果',merged_df)
        return merged_df


    def data_procss(self):
        newdata = self.northfound()
        final_data = pd.merge(self.df, newdata, on='证券代码', how='left')
        final_data = final_data.sort_values(by=['一级行业', '二级行业', '三级行业', '证券代码'])
        final_data = final_data.dropna()
        final_data = final_data.drop_duplicates(subset=['证券代码'])
        print(self.dates_str)
        #取日月环比，3个月环比,半年环比
        final_data['日月环比%'] = round((final_data[self.dates_str[-1]+'持股数量(万股)'] / final_data[self.dates_str[-2]+'持股数量(万股)']-1)*100, 2)
        final_data['3个月环比%'] = round((final_data[self.dates_str[-2] + '持股数量(万股)'] / final_data[self.dates_str[-5] + '持股数量(万股)'] - 1) * 100, 2)
        final_data['半年环比%'] = round((final_data[self.dates_str[-2] + '持股数量(万股)'] / final_data[self.dates_str[-7] + '持股数量(万股)'] - 1) * 100, 2)
        final_data['小记'] =final_data['日月环比%']+final_data['3个月环比%']+final_data['半年环比%']
        final_data = final_data.reset_index(drop=True)
        return final_data

if __name__ == '__main__':

    #输入上一交易日date,自动提取出最后前6个月的时间点
    date ='20240602'
    data = Getdata(date)
    final_data = data.data_procss()
    print(final_data)
    final_data.to_excel(date+'_全市场北向持仓量.xlsx',index=False)
