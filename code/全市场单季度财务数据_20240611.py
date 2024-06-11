import pandas as pd
import tushare as ts
import datetime

# 新增

# 增加国内国际占比利润比率
# 股息率



class Getdata():
    def __init__(self,start_date,end_date):
        self.df = pd.read_excel('行业匹配表.xlsx')
        self.start_date = start_date
        self.end_date =end_date
        self.pro = ts.pro_api('your_token')


    def getyjbb(self):
        # q_gr_yoy:营业总收入同比增长率(%)(单季度)
        # q_netprofit_yoy:归属母公司股东的净利润同比增长率(%)(单季度)
        # q_netprofit_qoq:归属母公司股东的净利润环比增长率(%)(单季度)
        # q_gr_qoq :营业总收入环比增长率(%)(单季度)
        # roe:净资产收益率
        # q_gsprofit_margin：销售毛利率(单季度)
        # q_profit_to_gr 净利润／营业总收入(单季度)
        # dt_netprofit_yoy:归属母公司股东的净利润 - 扣除非经常损益同比增长率(%)
        # extra_item:非经常性损益
        # profit_dedt:扣除非经常性损益后的净利润
        # q_dtprofit :扣除非经常损益后的单季度净利润
        print(f'正在获取{self.end_date}业绩报表数据')
        # 取归母净利润
        df_net_q_last = self.pro.income_vip(report_type=2, period=self.start_date,fields='ts_code,n_income_attr_p')
        df_net_q = self.pro.income_vip(report_type=2, period=self.end_date,fields='ts_code,n_income_attr_p')
        df_net_q_last.columns=  ['证券代码','归母净利润上期']
        df_net_q.columns = ['证券代码', '归母净利润本期']
        df_net=pd.merge(df_net_q,df_net_q_last,on='证券代码',how='left')
        print(df_net)


        #取去年单季度扣非
        df_kf = self.pro.fina_indicator_vip(period=self.end_date, fields='ts_code,ann_date')
        input_date = datetime.datetime.strptime(self.end_date, '%Y%m%d')
        last_year_date = input_date.replace(year=input_date.year - 1)
        last_year_date_str = last_year_date.strftime('%Y%m%d')
        df_dt_netprofit = self.pro.fina_indicator_vip(period=last_year_date_str, fields='ts_code,q_dtprofit')
        df_dt_netprofit.columns=['证券代码', '扣非净利润上期']


        # 取本季度的数据
        df_data = self.pro.fina_indicator_vip(period=self.end_date, fields='ts_code,ann_date,q_gr_yoy,q_gr_qoq,q_netprofit_yoy,q_netprofit_qoq,q_roe,q_profit_to_gr,q_dtprofit')
        new_column_names = {
            'ts_code': '证券代码',
            'q_gr_yoy': '营收同比',
            'q_gr_qoq': '营收环比',
            'q_netprofit_yoy': '净利润同比',
            'q_netprofit_qoq': '净利润环比',
            'q_roe': '净资产收益率年化',
            'q_profit_to_gr': '净利润率',
            'q_dtprofit': '扣非净利润本期',
            'ann_date': '最新公告日'
        }
        # 使用rename方法对多列进行重命名
        df_renamed = df_data.rename(columns=new_column_names)
        df= pd.merge(df_net,df_renamed,on='证券代码')
        df = df[~df['证券代码'].str.contains('BJ|退')]
        df =pd.merge(df,df_dt_netprofit,on='证券代码',how='left')
        df['扣非净利润同比']=round((df['扣非净利润本期']-df['扣非净利润上期'])/abs(df['扣非净利润上期'])*100,2)
        df =df[['证券代码','最新公告日','归母净利润上期','归母净利润本期','净利润同比','净利润环比','营收同比','营收环比','净资产收益率年化','净利润率','扣非净利润同比']]

        return df

    def zyywfx(self):
        offset = 0
        limit = 10000  # 假设每次请求限制为10000条数据
        all_data = pd.DataFrame()

        while True:
            df = self.pro.fina_mainbz_vip(period='20231231', type='D', offset=offset, limit=limit,
                                          fields='ts_code,end_date,bz_item,bz_sales,bz_profit')
            print(f"Retrieved {len(df)} rows")

            if df.empty:  # 如果没有数据返回，中断循环
                break

            all_data = pd.concat([all_data, df], ignore_index=True)
            offset += limit  # 更新偏移量以获取下一页数据

        print("Finished retrieving all pages")
        if len(all_data)>0:
            all_data = all_data[~all_data['ts_code'].str.contains('BJ|A')]
            all_data['bz_item'] = all_data['bz_item'].apply(lambda x: x if x in ["中国大陆", "国外", "其他业务(地区)"] else "中国大陆")
            #数据透视国内外占比
            pivot_data = pd.pivot_table(all_data, index='ts_code', columns='bz_item', values='bz_profit', aggfunc='sum')

            pivot_data.fillna(0, inplace=True)
            pivot_data['sum']=pivot_data['国外']+pivot_data['中国大陆']
            pivot_data['国内占比'] =round(pivot_data['中国大陆']/pivot_data['sum']*100,2)
            pivot_data['国外占比'] = round(pivot_data['国外'] / pivot_data['sum'] * 100, 2)
            pivot_data = pivot_data[['国内占比','国外占比']]
            pivot_data =pivot_data.reset_index()
            pivot_data.columns=['证券代码','国内占比','国外占比']
            return pivot_data
        else:
            return

    # 股息率
    def gxl(self):
        df = self.pro.query('daily_basic', ts_code='', trade_date=today,
                       fields='ts_code,dv_ratio')
        df['dv_ratio'] =round(df['dv_ratio'],2)
        df.columns=['证券代码','股息率']
        return df

    def data_procss(self):
        newdata = self.getyjbb()
        final_data = pd.merge(self.df, newdata, on='证券代码',how='right')
        final_data = final_data.sort_values(by=['一级行业', '二级行业', '三级行业', '证券代码'])
        # final_data = final_data.dropna()
        final_data = final_data.drop_duplicates(subset = ['证券代码'])
        final_data = final_data.reset_index(drop=True)
        final_data['归母净利润上期'] = round(final_data['归母净利润上期'] / 100000000, 2)
        final_data['归母净利润本期'] = round(final_data['归母净利润本期'] / 100000000, 2)
        final_data['净资产收益率年化'] = round(final_data['净资产收益率年化']*4 , 2)
        final_data.iloc[:, 6:] = final_data.iloc[:, 6:].round(2)

        #利润占比
        try:
            lrzb = self.zyywfx()
            final_data = pd.merge(final_data, lrzb, on='证券代码', how='left')
        except Exception as e:
            print(f"Error merging profit data: {e}")

        # 股息率
        gxl = self.gxl()
        final_data = pd.merge(final_data, gxl, on='证券代码',how='left')

        return final_data

if __name__ == '__main__':

    #上期报告期时间
    start_date = '20231231'
    #本期报告期时间
    end_date ='20240331'
    #此时跑代码的时间
    today = '20240611'

    data = Getdata(start_date,end_date)
    final_data = data.data_procss()
    print(final_data)
    final_data.to_excel(today+'_全市场单季度财务数据.xlsx',sheet_name=end_date+'单季度财务数据',index=False)

    #国内国际利润比率（主营构成分析经营分析）增加日期
    #增加国内国际占比利润比率
    #股息率
