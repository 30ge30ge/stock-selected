import pandas as pd
import requests
import json
import numpy as np
import tushare as ts
import datetime
import akshare as ak
from tqdm import tqdm
# 新增
# 3日涨跌幅




class Getdata():
    def __init__(self,stock_df,start_date,end_date):
        self.stock_df = stock_df
        self.start_date = start_date
        self.end_date =end_date
        self.mov = [5, 10, 20, 60]
        self.pro = ts.pro_api('12bcc63e716b0b807c7f1b8dc6e58570a2d45cdf8dd4b7296cb3d8ab')
        self.date = date


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
        # print(df_net)


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

    # 动态市盈率
    def pe(self):
        df = ak.stock_zh_a_spot_em()
        # 提取所需列
        df['证券代码'] = df['代码'].astype(str).str.zfill(6)
        # 和行业匹配表匹配,并取所需的列
        df['证券代码'] = df['证券代码'].apply(lambda X: X[:6] + '.SH' if X[0] == "6" else X[:6] + '.SZ')
        df =df[['证券代码','市盈率-动态']]
        return df

        # 北向数据
    def northfound(self):
        # 获取交易日历
        df_date = self.pro.hk_tradecal(exchange='', start_date='20231001', end_date=self.date)
        df_date = df_date.sort_values(by='cal_date')
        df_date = df_date[df_date['cal_date'] <= self.date]
        df_date = df_date[df_date['is_open'] == 1]
        # 将 'cal_date' 列转换为 datetime 类型
        df_date['cal_date'] = pd.to_datetime(df_date['cal_date'])
        # 找到每个月的最后一天
        last_day_of_month = df_date.resample('M', on='cal_date')['cal_date'].max().tolist()
        dates_str = [date.strftime('%Y%m%d') for date in last_day_of_month]
        self.dates_str = dates_str
        merged_df = pd.DataFrame()

        for i in tqdm(dates_str):
            print('获取{}日期北向数据'.format(i))
            try:
                if i == self.date:
                    df = self.pro.hk_hold(trade_date=i)
                    df['vol'] = round(df['vol'] / 10000, 2)
                    df = df[['ts_code', 'vol', 'ratio']]
                    df.columns = ['证券代码', i + '持股数量(万股)', i + '持股占比(%)']
                else:
                    df = self.pro.hk_hold(trade_date=i)
                    df['vol'] = round(df['vol'] / 10000, 2)
                    df = df[['ts_code', 'vol']]
                    # df = df[['股票代码', '持股数量']]
                    df.columns = ['证券代码', i + '持股数量(万股)']
                # df['证券代码'] = df['证券代码'].apply(lambda X: X[:6] + '.SH' if X[0] == "6" else X[:6] + '.SZ')
                if not merged_df.empty:
                    merged_df = pd.merge(merged_df, df, on='证券代码', how='outer')
                else:
                    merged_df = df.copy()
            except Exception as e:
                print(f'获取{dates_str}日期数据出错：{e}')
                continue
        # print('最终结果',merged_df)
        merged_df['日月环比%'] = round((merged_df[self.dates_str[-1] + '持股数量(万股)'] / merged_df[
            self.dates_str[-2] + '持股数量(万股)'] - 1) * 100, 2)
        merged_df['3个月环比%'] = round((merged_df[self.dates_str[-2] + '持股数量(万股)'] / merged_df[
            self.dates_str[-5] + '持股数量(万股)'] - 1) * 100, 2)
        merged_df['前3个月环比%'] = round((merged_df[self.dates_str[-5] + '持股数量(万股)'] / merged_df[
            self.dates_str[-7] + '持股数量(万股)'] - 1) * 100, 2)
        merged_df['小记'] = merged_df['日月环比%'] + merged_df['3个月环比%'] + merged_df['前3个月环比%']
        print('北向持仓',merged_df)
        return merged_df

    # 均线数据
    def ma(self):
        # 获取交易日历
        print(f'正在获取{sheet_name}均线数据')
        df_date = self.pro.trade_cal(exchange='', start_date='20230901', end_date=self.date)
        df_date = df_date.sort_values(by='cal_date')
        df_date = df_date[df_date['cal_date'] <= self.date]
        df_date = df_date[df_date['is_open'] == 1].tail(65)
        # 交易日历筛选排序
        date_list = df_date['cal_date'].tolist()

        result_df = pd.DataFrame()
        for i in tqdm(self.stock_df['证券代码']):
            try:
                # 获取股票k线
                df = ak.stock_zh_a_hist(i[:6], period='daily', start_date=date_list[-62], end_date=self.date,
                                        adjust='qfq')
                df = df[['日期', '开盘', '最高', '最低', '收盘', '成交量']]
                df['证券代码'] = i
                columns = ['time', 'open', 'high', 'low', 'close', 'volume', '证券代码']
                df.columns = columns
                # 计算移动平均线
                for window in self.mov:
                    df[f'ma_{window}'] = round(df['close'].rolling(window=window).mean(), 2)
                # 将计算结果添加到结果数据框
                df = df.tail(1)
                result_df = pd.concat([result_df, df])
            except:
                continue

        # 生成新列
        result_df['ma_5_10_20>ma_60'] = np.where(
            (result_df['ma_5'] > result_df['ma_60']) & (result_df['ma_10'] > result_df['ma_60']) & (
                        result_df['ma_20'] > result_df['ma_60']), '是', '否')
        result_df['ma_5_10>ma_20'] = np.where(
            (result_df['ma_5'] > result_df['ma_20']) & (result_df['ma_10'] > result_df['ma_20']), '是', '否')
        result_df['ma_5>ma_10'] = np.where(result_df['ma_5'] > result_df['ma_10'], '是', '否')

        # 删掉不需要的列
        result_df.drop(['close', 'open', 'high', 'low', 'volume'], axis=1, inplace=True)
        result_df['涨幅'] = round((result_df['ma_5'] - result_df['ma_60']) / result_df['ma_60'] * 100, 2)
        return result_df


    def gdhs(self):
        print('正在获取股东数据')
        stock_gdhs_new = ak.stock_zh_a_gdhs(symbol='最新')
        stock_gdhs_new['证券代码'] = stock_gdhs_new['代码'].apply(
            lambda X: X[:6] + '.SH' if X[0] == "6" else X[:6] + '.SZ')
        stock_gdhs_new['总市值'] = round(stock_gdhs_new['总市值'] / 100000000, 2)
        stock_gdhs_new['户均持股市值(万)'] = round(stock_gdhs_new['户均持股市值'] / 10000, 2)
        merged_df = stock_gdhs_new[['证券代码', '股东户数-本次', '总市值', '户均持股市值(万)']]
        merged_df.columns = ['证券代码', '股东户数-最新', '总市值(亿)', '户均持股市值(万)']

        # 取前3季度末
        datetime = ['20230930', '20231231', '20240331']
        for i in datetime:
            try:
                df = ak.stock_zh_a_gdhs(symbol=i)
                df = df[['代码', '股东户数-本次']]
                df.columns = ['证券代码', i + '股东户数']
                df['证券代码'] = df['证券代码'].apply(lambda X: X[:6] + '.SH' if X[0] == "6" else X[:6] + '.SZ')
                if not merged_df.empty:
                    merged_df = pd.merge(merged_df, df, on='证券代码', how='outer')
                else:
                    merged_df = df.copy()
            except Exception as e:
                print(f'获取{i}日期数据出错：{e}')
                continue

        merged_df['股东户数3个月环比'] = round(
            (merged_df['20240331股东户数'] - merged_df['20231231股东户数']) / merged_df['20231231股东户数'] * 100, 2)
        merged_df['股东户数前3个月环比'] = round(
            (merged_df['20231231股东户数'] - merged_df['20230930股东户数']) / merged_df['20230930股东户数'] * 100, 2)
        return merged_df




    def data_procss(self):
        newdata = self.getyjbb()
        final_data = pd.merge(self.stock_df, newdata, on='证券代码',how='left')
        final_data = final_data.sort_values(by=['一级行业', '二级行业', '三级行业', '证券代码', '证券代码'])

        final_data = final_data.dropna()
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
            gxl = self.gxl()
            final_data = pd.merge(final_data, gxl, on='证券代码', how='left')
            pe = self.pe()
            final_data = pd.merge(final_data, pe, on='证券代码', how='left')
            # 获取北向数据
            northdata = self.northfound()
            final_data = pd.merge(final_data, northdata, on='证券代码', how='left')
            # 获取均线数据
            madata = self.ma()
            final_data = pd.merge(final_data, madata, on='证券代码', how='left')
            # 获取股东户数数据
            gdhs = self.gdhs()

            final_data = pd.merge(final_data, gdhs, on='证券代码', how='left')

        except Exception as e:
            print(f"Error merging profit data: {e}")

        return final_data

if __name__ == '__main__':
    hyppb = pd.read_excel('行业匹配表.xlsx')
    #用来跑北向
    date ='20240630'

    #上期报告期时间
    start_date = '20231231'
    #本期报告期时间
    end_date ='20240331'
    #此时跑代码的时间
    today = '20240709'

    # 概念
    gainian = ['人脑工程', '转基因', '粮食概念', '地热能', '低碳冶金', '机器人执行器', '券商概念', '在线旅游', '中特估',
               '科创板做市商', '核污染防治', 'GDR', '航母概念', '农业种植', '抽水蓄能', '海洋经济', '光伏高速公路',
               '液冷概念', '同步磁阻电机', '绿色电力', '超超临界发电', 'IPv6', '华为欧拉', '钒电池', '东盟自贸区概念',
               'AH股',
               '跨境支付', '阿兹海默', '基本金属', '中字头', '生物质能发电', '小米汽车', 'RCEP概念', 'IPO受益',
               '参股期货',
               '钛白粉', '医废处理', '碳交易', '央企改革', '胎压监测', '磷化工', '东北振兴', '煤化工', '证金持股',
               '油气设服',
               '中证500', '广电', '减速器', '上证50_', '生态农业', '疫苗冷链', '草甘膦', '页岩气', '风能', '新能源',
               '水产养殖',
               '人造太阳', '轮毂电机', '汽车拆解', '稀缺资源', '空气能热泵', '核能核电', '分拆预期', '央视50_',
               '乡村振兴',
               'REITs概念', '熔盐储能', '水利建设', 'MSCI中国', '换电概念', '参股券商', '破净股', '量子通信',
               '2025规划', '无线充电',
               '星闪概念', '青蒿素', '数据确权', '湖北自贸', '汽车一体化压铸', '工业母机', '血氧仪', '深成500',
               '上证380', '发电机概念', '一带一路',
               '无人驾驶', '重组蛋白', '海工装备', '天然气', '创业成份', '刀片电池', '虚拟电厂', '上证180_',
               '飞行汽车(eVTOL)', '商汤概念', 'HS300_', '京东金融', '国企改革', '军民融合', '债转股', 'PVDF概念',
               '第四代半导体', '储能', '标准普尔', '可燃冰', '维生素', '油价相关', '沪股通', '富时罗素', '净水概念',
               '互联金融', '土壤修复',
               '互联医疗', '多模态AI', '大飞机', '钙钛矿电池', '蚂蚁概念', '充电桩', 'PPP模式', '鸡肉概念', '超导概念',
               '地摊经济', '京津冀', '黄金概念', '民爆概念', '深证100R', 'B股', '麒麟电池', '小金属概念', '深股通',
               'VPN', '汽车热管理', '高压快充', '单抗概念', '参股新三板', '基金重仓', '环氧丙烷', '铁路基建',
               '燃料电池', '屏下摄像',
               'EDA概念', '基因测序', '纾困概念', '无线耳机', '猪肉概念', '参股银行', '啤酒概念', 'MLCC', '地下管网',
               '参股保险',
               '退税商店', '氢能源', '盐湖提锂', '创投', '化工原料', 'ChatGPT概念', 'CAR-T细胞疗法', '彩票概念',
               '雄安新区', '共享经济', '北交所概念', 'RCS概念', '军工', '激光雷达', '免疫治疗', '成渝特区', 'PLC概念',
               '新能源车',
               'AIGC概念', '降解塑料', '华为汽车', '股权激励', '垃圾分类', '节能环保', '生物疫苗', '智能电网',
               '工业互联',
               '社保重仓', '土地流转', '太阳能', '超清视频', '传感器', '长江三角', 'UWB概念', '调味品概念', '3D玻璃',
               'MLOps概念',
               '特斯拉', '智能机器', '机器人概念', '边缘计算', '乳业', '空间站概念', '航天概念', '独角兽', '气溶胶检测',
               '工业4.0', '人造肉', '新冠检测', '食品安全', '养老金', '融资融券', '电子烟', '信创', '预盈预增',
               'Web3.0', '车联网',
               '氮化镓', '数据中心', '电子后视镜', '智能穿戴', '低价股', '超级电容', '尾气治理', '动力电池回收',
               '注射器概念',
               '阿里概念', '远程办公', '电子车牌', '算力概念', '通用航空', '智能电视', '独家药品', '毛发医疗',
               '复合集流体',
               '机器视觉', '苹果概念', '辅助生殖', '天基互联', '电子纸概念', '数字孪生',
               '送转预期', '体外诊断', '小米概念', '幽门螺杆菌概念', '云计算', '快递概念', '抗原检测', '物联网',
               '无人机', '固态电池', '存储芯片', '被动元件', '精准医疗', '转债标的', '国产软件', '大数据', '北斗导航',
               '华为概念', '知识产权', '国家安防', '数字水印', '医疗器械概念', '东数西算', '培育钻石', '特高压',
               '高送转', '粤港自贸', '核酸采样亭', '智能家居', '杭州亚运会', '汽车芯片', '区块链', '网络安全', '石墨烯',
               '预制菜概念', '3D摄像头', '健康中国', '人工智能', '网红直播', '5G概念', '数据要素', '鸿蒙概念',
               '华为昇腾', '百度概念', '病毒防治', '代糖概念', '百元股', '熊去氧胆酸', '锂电池', '磁悬浮概念', '创新药',
               '数字哨兵', '生物识别', '数字经济', 'PCB', '抗菌面料', '拼多多概念', '工业气体', 'AB股', '肝炎概念',
               '医疗美容', '茅指数', '口罩', '深圳特区', '氟化工', '电商概念', 'LED', 'TOPCon电池', '海绵城市', 'CRO',
               '元宇宙概念', '第三代半导体', '光伏建筑一体化', '世界杯', '供销社概念', '肝素概念', '新零售', '内贸流通',
               '6G概念', '蝗虫防治', '虚拟数字人', '专精特新', '碳基材料', '3D打印', '智慧城市', '工程机械概念',
               '痘病毒防治',
               '新型工业化', '影视概念', 'C2M概念', '全息技术', 'WiFi', '低空经济',
               '数字阅读', '北京冬奥', '电子身份证', '钠离子电池', '毫米波概念', '时空大数据', '智慧政务', '国产芯片',
               '新材料', '数字货币', 'ETC', '数据安全', '流感', '举牌', '国资云概念', '碳化硅', '工业大麻', '冷链物流',
               '噪声防治', '蒙脱石散', '短剧互动游戏', '虚拟现实', '超级真菌', '消毒剂', '预亏预减', 'OLED', '植物照明',
               '新冠药物', '贬值受益', '中药概念', '统一大市场', 'BC电池', '创业板综', '新型城镇化', '蓝宝石',
               '长寿药', '增强现实', '万达概念', '机构重仓', '白酒', 'NFT概念', '富士康', '快手概念', '空间计算',
               '免税概念', '养老概念', '超级品牌', '科创板做市股', 'MicroLED', '装配建筑', '职业教育', 'EDR概念',
               '混合现实', '字节概念', '半导体概念', '稀土永磁', '体育产业', 'MiniLED', 'PEEK材料概念', 'DRG/DIP',
               '在线教育', 'ERP概念', '壳资源', '婴童概念', '赛马概念', '社区团购', '中俄贸易概念', '宠物经济',
               '中超概念', '移动支付',
               '股权转让', '智慧灯杆', '沪企改革', '手游概念',
               'ST股', '滨海新区', '户外露营', 'QFII重仓', 'AI芯片', '地塞米松', '有机硅', '宁组合', '裸眼3D',
               '抖音小店', '千金藤素', 'IGBT概念', '氦气概念', 'HIT电池', '减肥药', '网络游戏', '电子竞技', '云游戏',
               '中芯概念', 'F5G概念', '化妆品概念', 'CPO概念', '租售同权', '光通信模块', '建筑节能', '纳米银',
               '托育服务', '光刻机(胶)', '跨境电商', 'SPD概念', '上海自贸', '盲盒经济', '昨日连板_含一字', '次新股',
               '注册制次新股', '昨日涨停_含一字', 'eSIM', '进口博览', '高带宽内存', 'Chiplet概念', '昨日触板',
               '昨日涨停']

    print('\033[91m请输入D50或者概念：\033[0m')
    sheet_name = input()
    if sheet_name == 'D50':
        df = pd.read_excel('20240710_业绩预告.xlsx')
        print(df)
        # df = df.drop('Unnamed: 0', axis=1)
        stock_df = df.copy()
    elif sheet_name in gainian:
        print('正在查询概念成分股')
        df = ak.stock_board_concept_cons_em(symbol=sheet_name)
        # 提取所需列
        df['证券代码'] = df['代码'].astype(str).str.zfill(6)
        # 和行业匹配表匹配,并取所需的列
        df['证券代码'] = df['代码'].apply(lambda X: X[:6] + '.SH' if X[0] == "6" else X[:6] + '.SZ')
        # 取0，3，6开头的股票
        df = df[df['证券代码'].astype(str).str.startswith(('0', '3', '6'))]
        df = pd.merge(df, hyppb, on='证券代码', how='left')
        df = df[['一级行业', '二级行业', '三级行业', '证券代码', '证券名称']]
        # print(df)
        stock_df = df.copy()

    data = Getdata(stock_df,start_date,end_date)
    final_data = data.data_procss()
    print(final_data)
    final_data.to_excel(today+sheet_name+'_财务北向均线股东户数数据.xlsx',sheet_name=end_date+'_财务北向均线股东户数数据',index=False)

