import pandas as pd
import tushare as ts
from datetime import datetime, timedelta
import asyncio
from tqdm import tqdm
import akshare as ak


class Getreportdata:
    def __init__(self,start_date,hyppb):
        self.start_date  = start_date
        self.df = hyppb

    def get_report(self):
        stock_report = ak.stock_repurchase_em()
        stock_report['证券代码'] =stock_report['股票代码'].apply(lambda X: X[:6] + '.SH' if X[0] == "6" else X[:6] + '.SZ')
        stock_report['计划回购金额区间-下限'] = round(stock_report['计划回购金额区间-下限'] / 100000000, 3)
        stock_report['计划回购金额区间-上限'] = round(stock_report['计划回购金额区间-上限'] / 100000000, 3)
        stock_report['已回购金额'] = round(stock_report['已回购金额'] / 100000000, 3)
        stock_report = stock_report[['证券代码','计划回购金额区间-下限','计划回购金额区间-上限','回购起始时间','实施进度','已回购金额','最新公告日期']]
        stock_report['最新公告日期'] =pd.to_datetime(stock_report['最新公告日期'])
        stock_report['回购起始时间'] = pd.to_datetime(stock_report['回购起始时间'])
        stock_report = stock_report[stock_report['回购起始时间']>self.start_date]
        stock_report = stock_report[stock_report['证券代码'].astype(str).str.startswith(('0', '3', '6'))]
        stock_report['最新公告日期'] = stock_report['最新公告日期'].dt.strftime('%Y-%m-%d')
        stock_report['回购起始时间'] = stock_report['回购起始时间'].dt.strftime('%Y-%m-%d')
        stock_report = stock_report.sort_values(by='最新公告日期', ascending=True).drop_duplicates(subset='证券代码', keep='last').reset_index(drop=True)

        return stock_report



    def data_process(self):
        try:
            stock_report = self.get_report()
        except Exception as e:
            print("Error occurred:", e)
            # 如果发生异常，则返回空DataFrame或者其他适当的值
            return pd.DataFrame()

        # 匹配索引数据
        final_data = pd.merge(self.df, stock_report, on='证券代码', how='right')
        final_data = final_data.sort_values(by=['最新公告日期','一级行业', '二级行业', '三级行业', '证券代码'])
        final_data = final_data.reset_index(drop=True)
        return final_data


if __name__ == '__main__':
    # 起始日期和结束日期
    start_date = '20240210'
    end_date = '20240320'
    hyppb = pd.read_excel('行业匹配表.xlsx')
    reportdata =Getreportdata(start_date,hyppb)
    data = reportdata.data_process()
    print(data)

    data.to_excel(end_date + '_回购公告.xlsx', index=False)

