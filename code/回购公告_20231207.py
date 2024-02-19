import pandas as pd
import warnings
# 忽略所有警告
warnings.simplefilter("ignore")
import numpy as np
import tushare as ts
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from tqdm import tqdm
import akshare as ak
# # symbol='财务报告'; choice of {"全部", "重大事项", "财务报告", "融资公告", "风险提示", "资产重组", "信息变更", "持股变动"}
# stock_notice_report_df = ak.stock_notice_report(symbol='财务报告', date="20220511")
# print(stock_notice_report_df)


class Getreportdata():
    def __init__(self,periods,end_date):
        self.pro  = ts.pro_api('your_token')
        self.periods = periods
        self.end_date = end_date
        self.df = pd.read_excel('20230930_业绩报告(单季度).xlsx')

    def getreportdata(self):
        data=[]
        dates = pd.date_range(end=self.end_date, periods=self.periods).strftime('%Y%m%d').tolist()
        for i in tqdm(dates):
            try:
                print(f'获取{i}的公告数据')
                stock_notice_report_df = ak.stock_notice_report(symbol='全部', date=i)
                stock_notice_report_df = stock_notice_report_df[stock_notice_report_df['公告类型'].str.contains('回购预案|增持|减持')]
                data.append(stock_notice_report_df)
            except:
                continue
        if data:
            final_data = pd.concat(data, ignore_index=True)
            final_data['证券代码'] = final_data['代码'].apply(lambda X: X[:6]+'.SH' if X[0] == "6" else X[:6]+'.SZ')
            print('收集完毕')
            data = self.gddata_process(final_data)
            data.drop_duplicates(subset=['证券代码','公告类型'], keep='first', inplace=True)
            return data

    def gddata_process(self,final_data):
        dataframe = self.df[['一级行业', '二级行业', '三级行业', '证券代码', '证券名称']]
        mergedata=pd.merge(dataframe,final_data,on='证券代码',how='right')
        mergedata =mergedata.sort_values(by=['一级行业', '二级行业', '三级行业'],ascending=False)
        return mergedata









if __name__ == '__main__':
    # 往前推多少天获取公告
    periods = 10
    # 截止日期
    end_date = '20231205'
    # 特定时间日期,取特定时间的数据
    data = Getreportdata(periods,end_date)
    newdata = data.getreportdata()
    print(newdata)
    newdata.to_excel(end_date + '_回购公告.xlsx', index=False)


# https://vip.stock.finance.sina.com.cn/corp/view/vCB_BulletinGather.php?page_index=5