import pandas as pd
from tqdm import tqdm
import akshare as ak
import warnings
# 忽略特定警告
warnings.filterwarnings("ignore", category=FutureWarning)

class Getdata:
    def __init__(self,end_date,hyppb):
        self.end_date  = end_date
        self.df = hyppb


    # 公司行为回购
    def pl(self):

        # df = ak.stock_report_disclosure(market="沪深京", period="2023年报")
        # df['初次变更'] = df['初次变更'].fillna(df['首次预约'])
        # df['二次变更'] = df['二次变更'].fillna(df['初次变更'])
        # df['三次变更'] = df['三次变更'].fillna(df['二次变更'])
        # df['证券代码'] = df['股票代码'].astype(str).apply(lambda x: x[:6] + '.SH' if x[0] == '6' else x[:6] + '.SZ')
        # df = df[['证券代码', '三次变更']]
        # df.columns = ['证券代码', '年报披露时间']
        df =ak.stock_yysj_em(symbol="沪深A股", date=end_date)
        df['一次变更日期'] = df['一次变更日期'].fillna(df['首次预约时间'])
        df['二次变更日期'] = df['二次变更日期'].fillna(df['一次变更日期'])
        df['三次变更日期'] = df['三次变更日期'].fillna(df['二次变更日期'])
        df['证券代码'] = df['股票代码'].astype(str).apply(lambda x: x[:6] + '.SH' if x[0] == '6' else x[:6] + '.SZ')
        df = df[['证券代码', '三次变更日期']]
        df.columns = ['证券代码', '年报披露时间']
        df['年报披露时间'] = df['年报披露时间'].dt.strftime('%Y-%m-%d')
        return df



    def data_process(self):
        try:
            df = self.pl()
        except Exception as e:
            print("Error occurred:", e)
            # 如果发生异常，则返回空DataFrame或者其他适当的值
            return pd.DataFrame()

        # 匹配索引数据
        final_data = pd.merge(self.df, df, on='证券代码', how='left')
        final_data = final_data.sort_values(by=['一级行业', '二级行业', '三级行业', '证券代码'])
        final_data = final_data.reset_index(drop=True)
        return final_data


if __name__ == '__main__':
    # 起始日期和结束日期
    hyppb = pd.read_excel('行业匹配表.xlsx')
    # 查寻报表时间【xxxx0331，xxxx0630，xxxx0930，xxxxx1231】
    end_date = '20240630'
    # 此时跑代码的时间
    today = '20240802'
    sheet_name = 'D50---二季度股票池V2'
    # 读取自定义文件
    df = pd.read_excel(sheet_name + '.xlsx')
    # print(df)
    stock_df = df.copy()
    data = Getdata(end_date,stock_df)
    final_data = data.data_process()
    final_data.to_excel(today +'自定义数据_'+end_date+'报表预披露.xlsx', index=False)
    print('保存完毕')
