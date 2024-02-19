import pandas as pd
import numpy as np
import tushare as ts
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from tqdm import tqdm
import akshare as ak
import datetime
import warnings
# 忽略特定警告
warnings.filterwarnings("ignore", category=FutureWarning)

#新增pb市净率



class Getpreddata():
    def __init__(self,last_date,start_date,end_date,date,pre_month,last_month,df,name):
        self.pro  = ts.pro_api('your_token')
        self.last_date = last_date
        self.start_date = start_date
        self.end_date = end_date
        self.date = date
        self.pre_month = pre_month
        self.last_month = last_month
        self.mov = [5, 10, 20, 60]
        self.name =name
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
        self.df =df



    # 财务指标数据
    def getfinancialdata(self):
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
        print('正在获取财务数据')
        # 取去年数据做单季度的扣非净利润同比
        df_lastdatedata = self.pro.fina_indicator_vip(period=last_date, fields='ts_code,q_dtprofit')
        df_lastdatedata.columns = ['ts_code', '扣非净利润(去年同期)']
        # 取上季度的净利润同比数据
        df_predata_q_netprofit_yoy = self.pro.fina_indicator_vip(period=start_date,
                                                 fields='ts_code,q_netprofit_yoy')
        # 取上季度的净利润数据
        df_predata_q_netprofit = self.pro.income_vip(period=start_date, report_type=2,
                                 fields='ts_code,n_income_attr_p')
        # 上季度归母净利润和净利润同比合并
        df_predata= pd.merge(df_predata_q_netprofit_yoy,df_predata_q_netprofit,on='ts_code')
        df_predata = df_predata[['ts_code', 'n_income_attr_p', 'q_netprofit_yoy']]
        # 把表名修改
        df_predata.columns = ['ts_code', '净利润(上季)', '净利润同比(上季单季度)']
        df_predata['净利润(上季)']=round(df_predata['净利润(上季)']/100000000,2)
        # 取本季度的数据
        df_data = self.pro.fina_indicator_vip(period=end_date, fields='ts_code,q_netprofit_qoq,ann_date,'
                                                                      'q_netprofit_yoy,q_gr_yoy,q_gr_qoq,q_profit_to_gr,'
                                                                      'q_roe,q_gsprofit_margin,dt_netprofit_yoy,profit_dedt,extra_item,q_dtprofit')
        # 归母净利润
        df_q_netprofit = self.pro.income_vip(period=end_date, report_type=2,
                                 fields='ts_code,n_income_attr_p')

        df_q_netprofit['净利润(本季)'] = round(df_q_netprofit['n_income_attr_p']/ 100000000, 2)
        #本季度数据合并
        data_bjd_merge = pd.merge(df_data, df_q_netprofit, on='ts_code', how='left')
        #本季度和上季度合并
        data_bjd_merge =pd.merge(data_bjd_merge, df_predata, on='ts_code', how='left')
        data_bjd_merge['q_roe'] =data_bjd_merge['q_roe']*4

        # 求单季度扣非净利润同比增长
        data_bjd_merge = pd.merge(data_bjd_merge, df_lastdatedata, on='ts_code', how='left')
        data_bjd_merge['扣非净利润同比(本季单季度)'] = (data_bjd_merge['q_dtprofit'] - data_bjd_merge[
            '扣非净利润(去年同期)']) / abs(data_bjd_merge['扣非净利润(去年同期)']) * 100
        data_bjd_merge['扣非净利润同比(本季单季度)'] = round(data_bjd_merge['扣非净利润同比(本季单季度)'], 2)
        # 添加pb数据
        stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
        df_pb =stock_zh_a_spot_em_df[['代码','市净率']]
        df_pb =df_pb.reset_index(drop=True)
        df_pb['ts_code'] = df_pb['代码'].apply(lambda X: X[:6] + '.SH' if X[0] == "6" else X[:6] + '.SZ')
        data_bjd_merge  = pd.merge(data_bjd_merge,df_pb,on='ts_code',how='left')


        # 取想要的列
        data_bjd_merge = data_bjd_merge[['ts_code', '净利润(上季)', '净利润(本季)', 'q_netprofit_qoq',
                                         '净利润同比(上季单季度)', 'q_netprofit_yoy', 'q_roe',
                                         'q_gr_yoy', 'q_gr_qoq', 'q_gsprofit_margin',
                                         'q_profit_to_gr', '扣非净利润同比(本季单季度)','市净率', 'ann_date']]

        cols = ['证券代码', '净利润(上季)', '净利润(本季)', '净利润环比增长',
                '净利润同比(上季单季度)', '净利润同比(本季单季度)', '净资产收益率(单季度*4)',
                '营业收入同比增长(本季单季度)', '营业收入环比增长(本季单季度)', '销售毛利率(单季度)',
                '净利润率(本季单季度)', '扣非净利润同比(本季单季度)', '市净率','最新公告日期']
        # 变成列
        data_bjd_merge.columns = cols
        data_bjd_merge.iloc[:, 3:-1] = data_bjd_merge.iloc[:, 3:-1].round(2)



        # 和初始化行业匹配 表合并
        data_all = pd.merge(self.df, data_bjd_merge, on='证券代码', how='left')
        # 去重
        data_all = data_all.drop_duplicates(subset='证券代码', keep='last')
        # 排序
        data_all = data_all.sort_values(by=['一级行业', '二级行业', '三级行业'])
        # 重制索引
        data_all = data_all.reset_index(drop=True)
        #取北向持股数据

        return data_all

    # 北向数据
    def northfound(self, pre_month, last_month, today,data):
        print('正在获取北向数据')
        df_date = self.pro.trade_cal(exchange='', start_date='20231201', end_date=today)
        df_date = df_date.sort_values(by='cal_date')
        df_date = df_date[df_date['cal_date'] <today]
        df_date = df_date[df_date['is_open'] == 1].tail(65)
        date_list = df_date['cal_date'].tolist()
        dates = [pre_month, last_month, date_list[-1]]
        hyppb =data.copy()
        # vol 持股数量(股)
        # ratio 持股占比（%），占已发行股份百分比
        for i in tqdm(dates):
            print('获取{}日期北向数据'.format(i))
            try:
                df = self.pro.hk_hold(trade_date=i)
                df = df[['ts_code','vol']]
                df['vol'] =round(df['vol']/10000,2)
                # print('2lie', df)
                df.columns = ['证券代码', i + '持股数量(万股)']
                hyppb = pd.merge(hyppb, df, on='证券代码', how='left')

            except:
                continue

        hyppb['最新持股变化率'] = round((hyppb.iloc[:, -1] - hyppb.iloc[:, -2]) / hyppb.iloc[:, -2] * 100, 2)
        return hyppb

    # 均线数据
    def get_klinedata(self,today,data):

        #获取交易日历
        print('正在获取均线数据')
        df_date = self.pro.trade_cal(exchange='', start_date='20230901', end_date=today)
        df_date =df_date.sort_values(by='cal_date')
        df_date =df_date[df_date['cal_date']<=today]
        df_date =df_date[df_date['is_open']==1].tail(65)
        #交易日历筛选排序
        date_list =df_date['cal_date'].tolist()

        result_df = pd.DataFrame()
        for i in tqdm(self.df['证券代码']):
            try:
                #获取股票k线
                df = ak.stock_zh_a_hist(i[:6], period='daily', start_date=date_list[-62], end_date=today,adjust='qfq')
                df = df[['日期', '开盘', '最高', '最低', '收盘', '成交量']]
                df['证券代码'] = i
                columns = ['time', 'open', 'high', 'low', 'close', 'volume', '证券代码']
                df.columns = columns
                # 计算移动平均线
                for window in self.mov:
                    df[f'ma_{window}'] = round(df['close'].rolling(window=window).mean(),2)
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
        result_df = pd.merge(data, result_df, on='证券代码', how='left')
        return result_df




    #盈利预测数据
    def getpreddata(self,df):
        data=[]
        print('正在获取盈利预测数据')
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
                # print(result_df)
                data.append(result_df)
            except:
                continue
        newdata=pd.concat(data)
        columns=['2024营收增长率','2025营收增长率',
                '2024净利润','2025净利润',
                '2024净利润增长率','2025净利润增长率',
                '2024roe','2025roe','证券代码']

        newdata.columns=columns
        newdata = newdata[['证券代码',
                           '2024营收增长率', '2024净利润', '2024净利润增长率', '2024roe',
                           '2025营收增长率', '2025净利润', '2025净利润增长率', '2025roe'
                           ]]
        # 去除亿和万的后缀并转换为以亿为单位的数字
        columns_to_process = ['2024净利润', '2025净利润']
        for column in columns_to_process:
            newdata[column] = newdata[column].apply(
                lambda x: round(float(x.strip('亿')) * 1 if '亿' in str(x) else float(x.strip('万')) / 10000,
                                2) if pd.notna(x) and x != '-' else pd.NA)
        newdata.iloc[:, 5:] = newdata.iloc[:, 5:].round(2)
        newdata.iloc[:, 1:] = newdata.iloc[:, 1:].round(2)
        data=df.copy()
        newdata=pd.merge(data,newdata,on='证券代码',how='left')
        return newdata

    #获取去年涨幅
    def yearkline(self, df):
        data = []
        print('正在获取去年涨幅数据')
        for i in tqdm(self.df['证券代码']):
            try:
                df_2022 = ak.stock_zh_a_hist(i[:6], period='daily', start_date='20221230', end_date='20221230', adjust='')
                df_2022['2022收盘'] = df_2022['收盘']
                df_2022['证券代码'] = i
                df_2023 = ak.stock_zh_a_hist(i[:6], period='daily', start_date='20231229', end_date='20231229', adjust='')
                df_2023['2023收盘'] = df_2023['收盘']
                df_2023['证券代码'] = i
                df_kline = pd.merge(df_2022, df_2023, on='证券代码')
                df_kline['去年涨幅'] = round((df_kline['2023收盘'] - df_kline['2022收盘']) / df_kline['2022收盘'] * 100, 2)
                data.append(df_kline)
            except:
                continue
        newdata = pd.concat(data)
        newdata =newdata[['去年涨幅', '证券代码']]
        newdata =pd.merge(df,newdata,on='证券代码')
        return newdata


    # 股东数据研究
    def getgddata(self):
        collected_rows = []
        print('正在获取股东数据')
        # 获取股东研究数据
        # self.df['symbol'] = self.df['证券代码'].apply(lambda X: 'sh' + X[:6] if X[0] == "6" else 'sz' + X[:6])
        for i in tqdm(self.df['证券代码']):
            symbol = 'sh' + i[:6] if i[0] == "6" else 'sz' + i[:6]
            try:
                stock_lt_top_10_em_df = ak.stock_gdfx_free_top_10_em(symbol=symbol, date=end_date)
                stock_top_10_em_df = ak.stock_gdfx_top_10_em(i, date=end_date)
                data = pd.concat([stock_lt_top_10_em_df, stock_top_10_em_df])
                data['证券代码'] = i
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
            final_data = final_data.drop_duplicates(subset=['证券代码', '变动比率'], keep='last')
            final_data['增减'] = final_data.apply(
                lambda row: '增加' if row['变动比率'] > 0 else ('减少' if row['变动比率'] < 0 else row['增减']), axis=1)
            # data =df.copy
            newdata = pd.merge(self.df, final_data, on='证券代码', how='left')
            newdata['变动比率'] =round(newdata['变动比率'],2)
            newdata = newdata.sort_values(by=['一级行业', '二级行业', '三级行业','证券代码'])
            return newdata


    def deal_data(self):
        name =self.name
        # 1.获取财务数据
        data =self.getfinancialdata()
        # 2.获取北向数据
        northfound_data =self.northfound(self.pre_month,self.last_month,self.date,data)
        # 3.获取均线数据
        ma_data =self.get_klinedata(self.date,northfound_data)
        # 4.获取业绩预测数据
        yjpred_data =self.getpreddata(ma_data)
        # 5.获取去年涨跌幅
        yearkline = self.yearkline(yjpred_data)
        # 6.获取股东数据
        gd_data = self.getgddata()
        print('正在保存文件xlsx')
        yearkline.to_excel(self.date + '_自定义概念数据.xlsx', sheet_name=name, index=False)
        # 写入 data_nb，作为第二个 sheet
        with pd.ExcelWriter(self.date + '_自定义概念数据.xlsx', engine='openpyxl', mode='a') as writer:
            gd_data.to_excel(writer, sheet_name=name+'_股东数据', index=False)
        return yjpred_data






if __name__ == '__main__':
    # 去年同期时间
    last_date = '20220930'
    # 起始日期
    start_date = '20230630'
    # 截止日期
    end_date = '20230930'
    # 北向持股前2个月日期
    pre_month = '20231229'
    last_month= '20240131'

    #概念
    gainian = ['人脑工程', '转基因', '粮食概念', '地热能', '低碳冶金', '机器人执行器', '券商概念', '在线旅游', '中特估',
     '科创板做市商', '核污染防治', 'GDR', '航母概念', '农业种植', '抽水蓄能', '海洋经济', '光伏高速公路', '液冷概念',
     '同步磁阻电机', '绿色电力', '超超临界发电', 'IPv6', '华为欧拉', '钒电池', '东盟自贸区概念', 'AH股', '跨境支付',
     '阿兹海默', '基本金属', '中字头', '生物质能发电', '小米汽车', 'RCEP概念', 'IPO受益', '参股期货', '钛白粉',
     '医废处理', '碳交易', '央企改革', '胎压监测', '磷化工', '东北振兴', '煤化工', '证金持股', '油气设服', '中证500',
     '广电', '减速器', '上证50_', '生态农业', '疫苗冷链', '草甘膦', '页岩气', '风能', '新能源', '水产养殖', '人造太阳',
     '轮毂电机', '汽车拆解', '稀缺资源', '空气能热泵', '核能核电', '分拆预期', '央视50_', '乡村振兴', 'REITs概念',
     '熔盐储能', '水利建设', 'MSCI中国', '换电概念', '参股券商', '破净股', '量子通信', '2025规划', '无线充电',
     '星闪概念', '青蒿素', '数据确权', '湖北自贸', '汽车一体化压铸', '工业母机', '血氧仪', '深成500', '上证380',
     '无人驾驶', '重组蛋白', '海工装备', '天然气', '创业成份', '刀片电池', '虚拟电厂', '上证180_', '发电机概念',
     '飞行汽车(eVTOL)', '商汤概念', 'HS300_', '京东金融', '国企改革', '军民融合', '债转股', 'PVDF概念', '第四代半导体',
     '标准普尔', '可燃冰', '维生素', '油价相关', '沪股通', '富时罗素', '净水概念', '互联金融', '土壤修复', '一带一路',
     '互联医疗', '多模态AI', '大飞机', '钙钛矿电池', '蚂蚁概念', '充电桩', 'PPP模式', '鸡肉概念', '超导概念', '储能',
     '地摊经济', '京津冀', '黄金概念', '民爆概念', '深证100R', 'B股', '麒麟电池', '小金属概念', '深股通', 'VPN',
     '汽车热管理', '高压快充', '单抗概念', '参股新三板', '基金重仓', '环氧丙烷', '铁路基建', '燃料电池', '屏下摄像',
     'EDA概念', '基因测序', '纾困概念', '无线耳机', '猪肉概念', '参股银行', '啤酒概念', 'MLCC', '地下管网', '参股保险',
     '退税商店', '氢能源', '盐湖提锂', '创投', '化工原料', 'ChatGPT概念', 'CAR-T细胞疗法', '彩票概念', '雄安新区',
     '共享经济', '北交所概念', 'RCS概念', '军工', '激光雷达', '免疫治疗', '成渝特区', 'PLC概念', '新能源车', 'AIGC概念',
     '降解塑料', '华为汽车', '股权激励', '垃圾分类', '节能环保', '生物疫苗', '智能电网', '工业互联', '社保重仓',
     '土地流转', '太阳能', '超清视频', '传感器', '长江三角', 'UWB概念', '调味品概念', '3D玻璃', 'MLOps概念', '特斯拉',
     '智能机器', '机器人概念', '边缘计算', '乳业', '空间站概念', '航天概念', '独角兽', '气溶胶检测', '工业4.0',
     '人造肉', '新冠检测', '食品安全', '养老金', '融资融券', '电子烟', '信创', '预盈预增', 'Web3.0', '车联网', '氮化镓',
     '数据中心', '电子后视镜', '智能穿戴', '低价股', '超级电容', '尾气治理', '动力电池回收', '注射器概念', '阿里概念',
     '远程办公', '电子车牌', '算力概念', '通用航空', '智能电视', '独家药品', '毛发医疗', '复合集流体', '机器视觉',
     '送转预期', '体外诊断', '小米概念', '幽门螺杆菌概念', '云计算', '快递概念', '抗原检测', '物联网', '苹果概念',
     '无人机', '固态电池', '存储芯片', '被动元件', '精准医疗', '转债标的', '国产软件', '大数据', '北斗导航', '辅助生殖',
     '华为概念', '知识产权', '国家安防', '数字水印', '医疗器械概念', '东数西算', '培育钻石', '特高压', '天基互联',
     '高送转', '粤港自贸', '核酸采样亭', '智能家居', '杭州亚运会', '汽车芯片', '区块链', '网络安全', '石墨烯',
     '预制菜概念', '3D摄像头', '健康中国', '人工智能', '网红直播', '5G概念', '数据要素', '鸿蒙概念', '电子纸概念',
     '华为昇腾', '百度概念', '病毒防治', '代糖概念', '百元股', '熊去氧胆酸', '锂电池', '磁悬浮概念', '创新药',
     '数字哨兵', '生物识别', '数字经济', 'PCB', '抗菌面料', '拼多多概念', '工业气体', 'AB股', '肝炎概念', '数字孪生',
     '医疗美容', '茅指数', '口罩', '深圳特区', '氟化工', '电商概念', 'LED', 'TOPCon电池', '海绵城市', 'CRO',
     '元宇宙概念', '第三代半导体', '光伏建筑一体化', '世界杯', '供销社概念', '肝素概念', '新零售', '内贸流通', '6G概念',
     '蝗虫防治', '虚拟数字人', '专精特新', '碳基材料', '3D打印', '智慧城市', '工程机械概念', '痘病毒防治', '新型工业化',
     '数字阅读', '北京冬奥', '电子身份证', '钠离子电池', '毫米波概念', '时空大数据', '智慧政务', '国产芯片', '影视概念',
     '新材料', '数字货币', 'ETC', '数据安全', '流感', '举牌', '国资云概念', '碳化硅', '工业大麻', '冷链物流', 'C2M概念',
     '噪声防治', '蒙脱石散', '短剧互动游戏', '虚拟现实', '超级真菌', '消毒剂', '预亏预减', 'OLED', '植物照明',
     '新冠药物', '贬值受益', '中药概念', '统一大市场', 'BC电池', '创业板综', '新型城镇化', '蓝宝石', '全息技术',
     '长寿药', '增强现实', '万达概念', '机构重仓', '白酒', 'NFT概念', '富士康', '快手概念', '空间计算', 'WiFi',
     '免税概念', '养老概念', '超级品牌', '科创板做市股', 'MicroLED', '装配建筑', '职业教育', 'EDR概念', '混合现实',
     '半导体概念', '稀土永磁', '体育产业', 'MiniLED', 'PEEK材料概念', 'DRG/DIP', '在线教育', 'ERP概念', '字节概念',
     '壳资源', '婴童概念', '赛马概念', '社区团购', '中俄贸易概念', '宠物经济', '中超概念', '移动支付', '股权转让',
     'ST股', '滨海新区', '户外露营', 'QFII重仓', 'AI芯片', '地塞米松', '有机硅', '宁组合', '裸眼3D', '智慧灯杆',
     '抖音小店', '千金藤素', 'IGBT概念', '氦气概念', 'HIT电池', '减肥药', '网络游戏', '电子竞技', '云游戏', '沪企改革',
     '中芯概念', 'F5G概念', '化妆品概念', 'CPO概念', '租售同权', '光通信模块', '建筑节能', '纳米银', '手游概念',
     '托育服务', '光刻机(胶)', '跨境电商', 'SPD概念', '上海自贸', '盲盒经济', '昨日连板_含一字', '次新股',
     '注册制次新股', '昨日涨停_含一字', 'eSIM', '进口博览', '高带宽内存', 'Chiplet概念', '昨日触板', '昨日涨停']
    # 概念成分股

    # 这里是指数成分股代码，沪深300，中证500，上证50，中证1000
    index_cons=['000300','000905','000016','000852']
    # 指数成分股（品种代码 ,品种名称 ,纳入日期）

    #本次脚本更改以下2个地方
    name = '券商概念'
    #这里是本次脚本修改的名字,看是否要指数成分还是概念，如果是自己创建的股票池的就写‘自创,然后在396行更改excel名称’
    date = '20240216'
    # 这里是生成excel表的日期，以及数据的日期

    # 判断是读取概念成分股还是指数成分股
    hyppb = pd.read_excel('行业匹配表.xlsx')
    if name in gainian:
        print('正在查询概念成分股')
        df = ak.stock_board_concept_cons_em(symbol=name)
        # 提取所需列
        df['证券代码'] = df['代码'].astype(str).str.zfill(6)
        # 和行业匹配表匹配,并取所需的列
        df['证券代码'] = df['证券代码'].apply(lambda X: X[:6] + '.SH' if X[0] == "6" else X[:6] + '.SZ')

        df = pd.merge(df, hyppb, on='证券代码', how='left')
        df = df[['一级行业', '二级行业', '三级行业', '证券代码', '证券名称']]

    elif name in index_cons:
        print('正在查询指数成分股')
        df =ak.index_stock_cons(symbol=name)
        # 提取所需列
        df['证券代码'] = df['品种代码'].astype(str).str.zfill(6)
        # 和行业匹配表匹配,并取所需的列
        df['证券代码'] = df['证券代码'].apply(lambda X: X[:6] + '.SH' if X[0] == "6" else X[:6] + '.SZ')

        df = pd.merge(df, hyppb, on='证券代码', how='left')
        df = df[['一级行业', '二级行业', '三级行业', '证券代码', '证券名称']]
    else:
        print('正在查询自己创建的股票池成分股')
        df = pd.read_excel('行业分析---无人驾驶.xlsx', header=1)
        # 提取所需列
        df['证券代码'] = df['证券代码'].astype(str).str.zfill(6)
        df['证券代码'] = df['证券代码'].apply(lambda X: X[:6] + '.SH' if X[0] == "6" else X[:6] + '.SZ')
        selected_columns = ['一级行业', '二级行业', '三级行业', '证券代码', '证券名称']
        df = df[selected_columns]
    print(df)


    data = Getpreddata(last_date,start_date,end_date,date,pre_month,last_month,df,name)
    datamerge = data.deal_data()
    print(datamerge)
    #去年涨幅添加








