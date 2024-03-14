import time
import requests
import pandas as pd
import akshare as ak

# def process_request():
#     url = "https://emappdata.eastmoney.com/stockrank/getAllCurrentList"
#     kwargs = {
#         "appId": "appId01",
#         "pageNo": 1,
#         "pageSize": "100",
#     }
#     result = requests.post(url, json=kwargs).json()
#     data = []
#     for i in result.get("data", []):
#         # print(i['sc'])
#         hisrank = ak.stock_hot_rank_latest_em(symbol=i['sc'])
#         hisRankChange  =hisrank.iloc[7,1]
#         i['hisRankChange'] = hisRankChange
#         data.append(i)
#
#     df = pd.DataFrame(data)
#
#     df.columns = ['代码', '排名', '实时变化','历史变化']
#     return df
#
#
#
#
# if __name__ == '__main__':
#     while True:
#         df = process_request()
#         # 在这里可以对DataFrame进行进一步处理或分析
#         print(df)
# #         time.sleep(10)


from tqdm import tqdm
import pandas as pd
import akshare as ak

def emrank():
    stock_hot_rank_em_df = ak.stock_hot_rank_em()
    select_data = []
    for i in tqdm(stock_hot_rank_em_df['代码']):
        hisrank = ak.stock_hot_rank_latest_em(symbol=i)
        hisRankChange = hisrank.iloc[7, 1]
        rankChange = hisrank.iloc[6, 1]
        select_data.append({'股票代码': i, '实时变化': rankChange, '历史变化': hisRankChange})
    select_df = pd.DataFrame(select_data)
    return select_df

if __name__ == '__main__':

    df = emrank()
    # 在这里可以对DataFrame进行进一步处理或分析
    print(df)
