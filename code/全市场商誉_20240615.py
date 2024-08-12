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
    def get_report(self):
        df = ak.stock_sy_em(date=self.end_date)
        print(df)
        df['证券代码'] =df['股票代码'].apply(lambda X: X[:6] + '.SH' if X[0] == "6" else X[:6] + '.SZ')
        df['商誉'] = round(df['商誉'] / 100000000, 3)
        df['上年商誉'] = round(df['上年商誉'] / 100000000, 3)
        df =df[['证券代码','商誉','上年商誉','公告日期','交易市场']]
        return df



    def data_process(self):
        try:
            df = self.get_report()
        except Exception as e:
            print("Error occurred:", e)
            # 如果发生异常，则返回空DataFrame或者其他适当的值
            return pd.DataFrame()

        # 匹配索引数据
        final_data = pd.merge(self.df, df, on='证券代码', how='left')
        final_data = final_data.sort_values(by=['公告日期','一级行业', '二级行业', '三级行业', '证券代码'])
        final_data = final_data.reset_index(drop=True)
        return final_data


if __name__ == '__main__':
    # 起始日期和结束日期
    hyppb = pd.read_excel('行业匹配表.xlsx')
    # 查寻年初至今的回购及股东增减持
    end_date = '20240331'
    # 此时跑代码的时间
    today = '20240615'
    df = hyppb[['一级行业', '二级行业', '三级行业', '证券代码', '证券名称']]
    # print(df)
    stock_df = df.copy()
    data = Getdata(end_date,stock_df)
    final_data = data.data_process()
    final_data.to_excel(today +'全市场_商誉.xlsx', sheet_name='商誉', index=False)
    print('保存完毕')


#加一个去年净利润
# 商誉/净利润

# 半年报时表
# 出预披露业绩的时间公告