import tushare as ts
import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime
# 此脚本计算个股的技术指标(5日，10日，20日，60日）
import akshare as ak


class Getdata():
    def __init__(self,today):
        # 时间
        self.mov=[5,10,20,60]
        self.today = today
        self.pro  = ts.pro_api('your_token')
        #初始化行业匹配表
        self.hyppb =pd.read_excel('行业匹配表.xlsx')

    # 获取北向数据akshare
    def get_klinedata(self,today):
        #获取交易日历
        df_date = self.pro.trade_cal(exchange='', start_date='20230901', end_date=today)
        df_date =df_date.sort_values(by='cal_date')
        df_date =df_date[df_date['cal_date']<=today]
        df_date =df_date[df_date['is_open']==1].tail(65)
        #交易日历筛选排序
        date_list =df_date['cal_date'].tolist()
        # print(date_list,date_list)
        result_df = pd.DataFrame()
        for i in tqdm(self.hyppb['证券代码']):
            try:
                #获取股票k线
                df = ak.stock_zh_a_hist(i[:6], period='daily', start_date=date_list[-62], end_date=today,adjust='qfq')
                df = df[['日期', '开盘', '最高', '最低', '收盘', '成交量']]
                df['证券代码'] = i
                columns = ['time', 'open', 'high', 'low', 'close', 'volume', '证券代码']
                df.columns = columns
                # 计算移动平均线
                for window in self.mov:
                    df[f'ma_{window}'] = df['close'].rolling(window=window).mean()
                # 将计算结果添加到结果数据框
                df=df.tail(1)
                result_df = pd.concat([result_df, df])
            except:
                continue

        # 生成新列
        result_df['ma_5_10_20>ma_60'] = np.where(
            (result_df['ma_5'] > result_df['ma_60']) & (result_df['ma_10'] > result_df['ma_60']) & (result_df['ma_20'] > result_df['ma_60']), '是', '否')
        result_df['ma_5_10>ma_20'] = np.where((result_df['ma_5'] > result_df['ma_20']) & (result_df['ma_10'] > result_df['ma_20']), '是', '否')
        result_df['ma_5>ma_10'] = np.where(result_df['ma_5'] > result_df['ma_10'], '是', '否')

        #删掉不需要的列
        result_df.drop(['open', 'high', 'low', 'volume'], axis=1, inplace=True)

        #合并
        result_df = pd.merge(self.hyppb, result_df, on='证券代码', how='left')

        return result_df

if __name__ == '__main__':

    #修改today的日期，其他不调整
    today='20231219'
    specified_date = datetime.datetime.strptime(today, '%Y%m%d')

    end_date = (datetime.datetime.now() + datetime.timedelta(days=0)).strftime('%Y%m%d')
    print(end_date)

    data = Getdata(today)
    result_df=data.get_klinedata(today)
    print(result_df)
    result_df.to_excel(today + '_均线数据监控.xlsx', index=False)





