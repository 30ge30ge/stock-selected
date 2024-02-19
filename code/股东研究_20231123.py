import pandas as pd
import numpy as np
import warnings
# 忽略所有警告
warnings.simplefilter("ignore")
import tushare as ts
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from tqdm import tqdm
import akshare as ak

class Getgddata():
    def __init__(self,start_date,end_date,date):
        self.pro  = ts.pro_api('your_token')
        # 初始化聪明资金
        self.smart_investors = [
            '澳门金融管理局', 'UBS AG', 'JPMORGAN', '香港中央结算有限公司', '全国社保基金',
            '阿布达比投资局', '魁北克储蓄投资集团', 'MORGAN STANLEY', '挪威中央银行',
            '新加坡华侨银行有限公司', '葛卫东', '陈发树', '上海重阳战略投资有限公司', '汇添富基金管理股份有限公司',
            '上海高毅资产管理合伙企业',
            '基本养老保险基金', '广发基金管理有限公司', '法国巴黎银行', '安本标准投资管理',
            '大成基金管理有限公司', '香港金融管理局', '科威特政府投资局','高盛'
        ]
        # 模糊匹配阈值
        self.fuzzy_threshold = 90
        self.df=pd.read_excel('20230930_业绩报告(单季度).xlsx')
        self.date = date
        self.start_date = start_date
        self.end_date = end_date

    def getgddata(self):
        collected_rows = []
        # 获取股东研究数据
        # self.df['symbol'] = self.df['证券代码'].apply(lambda X: 'sh' + X[:6] if X[0] == "6" else 'sz' + X[:6])
        for i in tqdm(self.df['证券代码'][:10]):
            symbol = 'sh' + i[:6] if i[0] == "6" else 'sz' + i[:6]
            try:
                stock_lt_top_10_em_df = ak.stock_gdfx_free_top_10_em(symbol=symbol, date=end_date)
                stock_top_10_em_df = ak.stock_gdfx_top_10_em(i, date=end_date)
                data=pd.concat([stock_lt_top_10_em_df,stock_top_10_em_df])
                data['证券代码']=i
                for name in data['股东名称']:
                    match, score = process.extractOne(name, self.smart_investors, scorer=fuzz.partial_ratio)
                    if score >= self.fuzzy_threshold:
                        # print("Matched: " + name)
                        collected_rows.append(data[data['股东名称'] == name])
            except:
                continue

        if collected_rows:
            final_data = pd.concat(collected_rows, ignore_index=True)
            print("Collected Rows:")
            final_data = final_data.drop_duplicates(subset=['证券代码','变动比率'], keep='last')
            final_data['增减'] = final_data.apply(lambda row: '增加' if row['变动比率'] > 0 else ('减少' if row['变动比率'] < 0 else row['增减']), axis=1)
            data=self.gddata_process(final_data)
            return data

    def gddata_process(self,final_data):

        dataframe = self.df[['一级行业', '二级行业', '三级行业', '证券代码', '证券名称']]
        mergedata=pd.merge(dataframe,final_data,on='证券代码',how='right')
        return mergedata








if __name__ == '__main__':
    # 起始日期
    start_date = '20230630'
    # 截止日期
    end_date = '20230930'
    # 特定时间日期,取特定时间的数据
    date = '20231031'
    data = Getgddata(start_date,end_date,date)
    newdata = data.getgddata()
    print(newdata)
    newdata.to_excel(end_date+'_股东研究1.xlsx',index=False)
    #申万3级行业成分股
    #指数成分股
    #概念成分股
