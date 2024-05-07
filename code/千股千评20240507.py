import akshare as ak
import pandas as pd

class Getdata():
    def __init__(self,code):
        self.code = code

    def dataprocess(self):
        stock_comment_em_df = ak.stock_comment_em()
        df=stock_comment_em_df[['代码','名称','最新价','涨跌幅','主力成本','机构参与度','上升','目前排名','关注指数']]
        df=df[df['代码']==self.code]
        return df


   #全市场
    def alldata(self):
        stock_comment_em_df = ak.stock_comment_em()
        df = stock_comment_em_df[
            ['代码', '名称', '最新价', '涨跌幅', '主力成本', '机构参与度', '上升', '目前排名', '关注指数']]
        df =df.sort_values('目前排名',ascending=True)
        return df






if __name__ == '__main__':


    # 接收用户输入:
    print('\033[91m请输入代码：\033[0m')
    code = input()
    data =Getdata(code)
    result=data.dataprocess()
    print(result)

