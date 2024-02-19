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


class Getpreddata():
    def __init__(self):
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
        self.df = pd.read_excel('行业匹配表.xlsx')

    def getpreddata(self):
        data=[]
        # data =pd.DataFrame(columns=['预测指标','2020-实际值','2021-实际值','2022-实际值','预测2023-平均','预测2024-平均','预测2025-平均'])
        for i in tqdm(self.df['证券代码']):
            try:
                stock_profit_forecast_ths_df = ak.stock_profit_forecast_ths(symbol=i[:6],
                                                                        indicator="业绩预测详表-详细指标预测")

                stock_profit_forecast_ths_df=stock_profit_forecast_ths_df.unstack()
                stock_profit_forecast_ths_df =stock_profit_forecast_ths_df.reset_index()
                # print(stock_profit_forecast_ths_df)
                stock_profit_forecast_ths_df.columns = ['index', '序号','值']

                stock_profit_forecast_ths_df=stock_profit_forecast_ths_df[['序号','值']]
                #‘营业收入增长率
                ys =stock_profit_forecast_ths_df[stock_profit_forecast_ths_df['序号']==1].tail(2)
                # 净利润绝对值
                jlr =stock_profit_forecast_ths_df[stock_profit_forecast_ths_df['序号']==3].tail(2)
                # 净利润增长率
                jlrpct =stock_profit_forecast_ths_df[stock_profit_forecast_ths_df['序号']==4].tail(2)
                # roe
                roe  =stock_profit_forecast_ths_df[stock_profit_forecast_ths_df['序号']==7].tail(2)

                # 将四个 DataFrame 按列拼接
                result_df = pd.concat([ys, jlr, jlrpct, roe], axis=0)
                result_df=result_df[['值']].T
                result_df['证券代码']=i

                #取涨跌幅
                yearkline =self.yearkline(i)
                result_df =pd.merge(result_df,yearkline,on='证券代码')
                data.append(result_df)
            except:
                continue
        newdata=pd.concat(data)
        columns = ['2024营收增长率', '2025营收增长率',
                   '2024净利润', '2025净利润',
                   '2024净利润增长率', '2025净利润增长率',
                   '2024roe', '2025roe', '证券代码','去年涨幅']

        newdata.columns = columns
        newdata = newdata[['证券代码',
                           '2024营收增长率', '2024净利润', '2024净利润增长率', '2024roe',
                           '2025营收增长率', '2025净利润', '2025净利润增长率', '2025roe','去年涨幅'
                           ]]
        # 去除亿和万的后缀并转换为以亿为单位的数字
        columns_to_process = ['2024净利润', '2025净利润']
        for column in columns_to_process:
            newdata[column] = newdata[column].apply(
                lambda x: round(float(x.strip('亿')) * 1 if '亿' in str(x) else float(x.strip('万')) / 10000,
                                2) if pd.notna(x) and x != '-' else pd.NA)

        percentage_columns = ['2024营收增长率', '2025营收增长率',
                              '2024净利润增长率', '2025净利润增长率',
                               '2024roe', '2025roe']
        percen_10 = ['2024净利润增长率', '2025净利润增长率']
        newdata =newdata.dropna()
        # 去除 '%' 并转换为浮点数
        for column in percentage_columns:
            newdata[column] = pd.to_numeric(newdata[column].str.replace('%', ''), errors='coerce')
            if column in percen_10:
                newdata[column] = self.quantile(newdata[column], 90, 10)
            else:
                newdata[column] = self.quantile(newdata[column], 98.5, 1.5)

        newdata.iloc[:, 5:] = newdata.iloc[:, 5:].round(2)
        newdata.iloc[:, 1:] = newdata.iloc[:, 1:].round(2)
        data=self.df[['一级行业', '二级行业', '三级行业', '证券代码', '证券名称']]
        newdata=pd.merge(data,newdata,on='证券代码',how='left')
        return newdata

    # 定义去极值函数
    def quantile(self, factor, up, down):
        if len(factor) > 0:
            up_scale = np.percentile(factor, up)
            down_scale = np.percentile(factor, down)
            factor = np.where(factor > up_scale, up_scale, factor)
            factor = np.where(factor < down_scale, down_scale, factor)
        return factor

    def yearkline(self,i):
        df_2022 = ak.stock_zh_a_hist(i[:6], period='daily', start_date='20221230', end_date='20221230', adjust='')
        df_2022['2022收盘'] =df_2022['收盘']
        df_2022['证券代码'] = i
        df_2023 = ak.stock_zh_a_hist(i[:6], period='daily', start_date='20231229', end_date='20231229', adjust='')
        df_2023['2023收盘'] = df_2023['收盘']
        df_2023['证券代码'] = i
        df_kline =pd.merge(df_2022,df_2023,on='证券代码')
        df_kline['去年涨幅'] =round((df_kline['2023收盘']-df_kline['2022收盘'])/df_kline['2022收盘']*100,2)
        return df_kline[['去年涨幅','证券代码']]




if __name__ == '__main__':
    data = Getpreddata()
    newdata = data.getpreddata()
    print(newdata)
    newdata.to_excel('20240216_业绩预测详细指标预测.xlsx')

