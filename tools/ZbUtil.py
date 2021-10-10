# -*-coding:utf-8-*-

import os
import numpy as np
import pandas as pd
from StUtil import StUtil
import scipy.signal as signal
import matplotlib.pyplot as plt
import time
import datetime
import threading
from time import sleep, ctime


class ZbUtil:
    def __init__(self, dir):
        self.root_dir = dir
        self.start_date = '20140101'
        self.end_date = '20181130'
        self.stocks_dir = os.path.join(self.root_dir, 'stocks')
        self.stocks_wk_dir = os.path.join(self.root_dir, 'stocks_wk')
        self.stock_dir = os.path.join(self.root_dir, 'stock')
        self.stock_today_dir = os.path.join(self.root_dir, 'stock')
        self.p230_dir = os.path.join(self.root_dir, 'p230')
        self.single_data_dir = os.path.join(self.root_dir, 'single_d')
        self.corr_dir = os.path.join(self.root_dir, 'corr')
        self.zb_dir = os.path.join(self.root_dir, 'zb')
        self.stUtil = StUtil(self.root_dir)
        self.calc_date = 'unknown'
        self.eva = [[0, 0] for i in range(20)]
        self.stock_list = []

    def set_calc_date(self, calc_date):
        self.calc_date = calc_date

    def calc_stock_kdj(self, ts_code, type):
        st_code = ts_code.split('.')[0]
        file = os.path.join(self.stocks_dir, st_code + '.csv')

        if type == 'week':
            file = os.path.join(self.stocks_wk_dir, st_code + '.csv')

        # cols = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol', 'amount']
        cols = ['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
        df = pd.read_csv(file, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        if df is None:
            return None

        df = df[-200:]
        try:
            low_list = df['low'].rolling(9, min_periods=9).min()
            low_list.fillna(value=df['low'].expanding().min(), inplace=True)
            high_list = df['high'].rolling(9, min_periods=9).max()
            high_list.fillna(value=df['high'].expanding().max(), inplace=True)
            rsv = (df['close'] - low_list) / (high_list - low_list) * 100

            df['K'] = pd.DataFrame(rsv).ewm(com=2).mean()
            df['D'] = df['K'].ewm(com=2).mean()
            df['J'] = 3 * df['K'] - 2 * df['D']
        except:
            print("calc kdj for %s failed!" % (ts_code))
            return None

        df = pd.concat((df, pd.DataFrame(columns=['KDJ_label', 'stock'])), axis=1)
        kdj_position = df['K'] > df['D']
        df.loc[kdj_position[(kdj_position == True) & (kdj_position.shift() == False)].index - 1, 'KDJ_label'] = '金叉'
        df.loc[kdj_position[(kdj_position == False) & (kdj_position.shift() == True)].index - 1, 'KDJ_label'] = '死叉'

        df_tail = df.iloc[-1:].tail(1).reset_index(drop=True)
        df_pre_tail = df.iloc[-2:-1].reset_index(drop=True)

        df_tail = df_tail.loc[:, ['trade_date', 'stock', 'K', 'D', 'J', 'KDJ_label']]
        df_tail.loc[0, 'stock'] = ts_code

        if df_tail.loc[0, 'K'] < 20 and df_tail.loc[0, 'D'] < 20 and df_tail.loc[0, 'J'] < 20:
            return df_tail
        elif df_tail.loc[0, 'KDJ_label'] == '金叉' or df_pre_tail.loc[0, 'KDJ_label'] == '金叉':
            if df_tail.loc[0, 'K'] < 30 and df_tail.loc[0, 'D'] < 30 and df_tail.loc[0, 'J'] < 30:
                df_tail.loc[0, 'KDJ_label'] = '金叉30'
            elif df_tail.loc[0, 'K'] < 40 and df_tail.loc[0, 'D'] < 40 and df_tail.loc[0, 'J'] < 40:
                df_tail.loc[0, 'KDJ_label'] = '金叉40'
            elif df_tail.loc[0, 'K'] < 50 and df_tail.loc[0, 'D'] < 50 and df_tail.loc[0, 'J'] < 50:
                df_tail.loc[0, 'KDJ_label'] = '金叉50'
            else:
                df_tail.loc[0, 'KDJ_label'] = '金叉50+'
            return df_tail
        else:
            return None

        '''
        print(df.loc[:, ['K', 'D', 'J']].head(20))
        print(df.loc[:, ['K', 'D', 'J']].tail(10))
        df['KDJ_金叉死叉'] = ''
        kdj_position = df['K'] > df['D']
        df.loc[kdj_position[(kdj_position == True) & (kdj_position.shift() == False)].index-1, 'KDJ_金叉死叉'] = '金叉'
        df.loc[kdj_position[(kdj_position == False) & (kdj_position.shift() == True)].index-1, 'KDJ_金叉死叉'] = '死叉'
        file_out = os.path.join(self.zb_dir, st_code+'.csv')
        df.to_csv(file_out, mode='w', index=False, encoding="utf_8_sig")
        '''

    def kdj_filter(self, type):
        df_out = None
        stocks = self.stUtil.get_all_stocks(type)
        for stock in stocks:
            df_stock = self.calc_stock_kdj(stock)
            if df_out is None:
                df_out = df_stock
            else:
                df_out = pd.concat((df_out, df_stock), axis=0)

        print(df_out)
        file_out = os.path.join(self.zb_dir, 'kdj_' + self.calc_date + '.csv')
        df_out.to_csv(file_out, mode='w', index=False, encoding="utf_8_sig")

    def kdj_wk_filter(self, type):
        df_out = None
        stocks = self.stUtil.get_all_stocks(type)
        for stock in stocks:
            df_stock = self.calc_stock_kdj(stock, 'week')
            if df_out is None:
                df_out = df_stock
            else:
                df_out = pd.concat((df_out, df_stock), axis=0)

        print(df_out)
        file_out = os.path.join(self.zb_dir, 'kdj_wk_' + self.calc_date + '.csv')
        df_out.to_csv(file_out, mode='w', index=False, encoding="utf_8_sig")

    @staticmethod
    def mean_filter_method_0(v_20, m_20):
        for i in range(17, 20):
            if v_20[i] > m_20[i]:
                return False

        if v_20[19] < v_20[18]:
            return False

        if v_20[18] < v_20[17]:
            return False

        if v_20[17] < v_20[16]:
            return False

        return True

    @staticmethod
    def mean_filter_method_1(v_20, m_20):
        if v_20[19] < v_20[18]:
            return False

        if v_20[18] < v_20[17]:
            return False

        for i in range(18, 20):
            if v_20[i] < m_20[i]:
                return False

        for i in range(13, 18):
            if v_20[i] > m_20[i]:
                return False

        return True

    @staticmethod
    def mean_filter_method_2(v_20, m_20):
        for i in range(17):
            if v_20[i] > m_20[i]:
                return False

        if v_20[19] < v_20[18]:
            return False

        if v_20[18] < v_20[17]:
            return False

        if v_20[17] < v_20[16]:
            return False

        if v_20[19] > m_20[19]:
            return True
        else:
            delta = m_20[19] - v_20[19]
            if (delta * 20) < m_20[19]:
                return True
            else:
                return False

        return True

    @staticmethod
    def mean_filter_method_3(v_20, m_20):
        for i in range(17):
            if v_20[i] > m_20[i]:
                return False

        if v_20[19] <= m_20[19]:
            return False

        return True

    def mean_filter_one_stock(self, result, ts_code):
        st_code = ts_code.split('.')[0]
        file_stock = os.path.join(self.stocks_dir, st_code + '.csv')

        # cols = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol', 'amount']
        cols = ['trade_date', 'close', 'vol', 'amount']
        df = pd.read_csv(file_stock, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        if df is None:
            return False

        df_39 = df['close'][-39:].reset_index()
        if df_39.shape[0] != 39:
            print "%s's data is wrong %d" % (ts_code, df_39.shape[0])
            return False

        v_20 = df_39['close'][-20:].values
        m_20 = []
        for i in range(20):
            m_20.append(df_39['close'][i:i+20].mean())

        if self.mean_filter_method_0(v_20, m_20):
            result[0].append(ts_code)
            print "M1 add %s" % ts_code

        if self.mean_filter_method_1(v_20, m_20):
            result[1].append(ts_code)
            print "M2 add %s" % ts_code

        return result

    def mean_filter_one_stock_new(self, result, ts_code, days):
        st_code = ts_code.split('.')[0]
        file_stock = os.path.join(self.stocks_dir, st_code + '.csv')

        # cols = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol', 'amount']
        cols = ['trade_date', 'close', 'vol', 'amount']
        df = pd.read_csv(file_stock, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        if df is None:
            return False

        if df.shape[0] < (19+days):
            print "%s's data is wrong %d" % (ts_code, df.shape[0])
            return False

        df1 = df['close'][-19-days:].reset_index()

        v_a = df1['close'][-20:].values
        m_a = []
        for i in range(20):
            m_a.append(df1['close'][i:i+days].mean())

        if self.mean_filter_method_2(v_a, m_a):
            result[0].append(ts_code)
            print "M2 add %s" % ts_code

        if self.mean_filter_method_3(v_a, m_a):
            result[1].append(ts_code)
            print "M3 add %s" % ts_code

        return result

    def mean_20_filter(self):
        result = [list(), list()]
        stocks = self.stUtil.get_all_stocks(type)
        for stock in stocks:
            self.mean_filter_one_stock(result, stock)

        print result[0]
        print result[1]
        len0 = len(result[0])
        len1 = len(result[1])

        len_max = max(len0, len1)
        for i in range(len_max - len0):
            result[0].append(0)

        for i in range(len_max - len1):
            result[1].append(0)
            
        file_out = os.path.join(self.zb_dir, 'mean_20_' + self.calc_date + '.csv')
        df_out = pd.DataFrame({'M0': result[0], 'M1': result[1]})
        df_out.to_csv(file_out, mode='w', index=False, encoding="utf_8_sig")

    def mean_filter(self, days):
        result = [list(), list()]
        stocks = self.stUtil.get_all_stocks(3)
        for stock in stocks:
            self.mean_filter_one_stock_new(result, stock, 100)

        print result[0]
        print result[1]
        len0 = len(result[0])
        len1 = len(result[1])

        len_max = max(len0, len1)
        for i in range(len_max - len0):
            result[0].append(0)

        for i in range(len_max - len1):
            result[1].append(0)

        file_out = os.path.join(self.zb_dir, 'mean_' + str(days) + '_' + self.calc_date + '.csv')
        df_out = pd.DataFrame({'M2': result[0], 'M3': result[1]})
        df_out.to_csv(file_out, mode='w', index=False, encoding="utf_8_sig")

    def guaidian_one_stock(self, ts_code):
        st_code = ts_code.split('.')[0]
        file_stock = os.path.join(self.stocks_dir, st_code + '.csv')

        # cols = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol', 'amount']
        cols = ['trade_date', 'close', 'vol', 'amount']
        df = pd.read_csv(file_stock, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        if df is None:
            return False

        df = df[-180:].reset_index()
        v = np.array(df['close'].values)

        len_v = len(v)
        for i in range(len_v-4, len_v-9):
            if len_v[i] > len_v[i+i]:
                return False

        #print
        #index = signal.argrelextrema(v, np.less_equal)

        top_idx = signal.argrelextrema(v, np.greater_equal)
        bottom_idx = signal.argrelextrema(v, np.less_equal)

        top_idx_len = len(top_idx[0])
        bottom_idx_len = len(bottom_idx[0])

        #print top_idx_len, top_idx
        #print bottom_idx_len, bottom_idx

        if bottom_idx[0][bottom_idx_len - 1] > top_idx[0][top_idx_len - 1]:
            return False

        if top_idx[0][top_idx_len - 1] - bottom_idx[0][bottom_idx_len - 1] < 2:
            return False

        if bottom_idx[0][bottom_idx_len - 1] - top_idx[0][top_idx_len - 2] < 5:
            return False

        if v[bottom_idx[0][bottom_idx_len - 1]] < v[bottom_idx[0][bottom_idx_len - 2]]:
            return False

        if v[bottom_idx[0][bottom_idx_len - 2]] < v[bottom_idx[0][bottom_idx_len - 3]]:
            return False

        #print v[signal.argrelextrema(v, np.less_equal)]
        #print signal.argrelextrema(v, np.less_equal)
        plt.figure(figsize=(16, 4))
        plt.title(ts_code)
        plt.plot(np.arange(len(v)), v)
        plt.plot(signal.argrelextrema(v, np.greater_equal)[0], v[signal.argrelextrema(v, np.greater_equal)], 'o')
        plt.plot(signal.argrelextrema(v, np.less_equal)[0], v[signal.argrelextrema(v, np.less_equal)], '+')
        plt.show()

        return True

    def guaidian_filter(self):
        result = []
        stocks = self.stUtil.get_all_stocks(type)
        for stock in stocks:
            if self.guaidian_one_stock(stock):
                result.append(stock)

        file_out = os.path.join(self.zb_dir, 'guaidian_' + self.calc_date + '.csv')
        df_out = pd.DataFrame({'guaidian': result})
        df_out.to_csv(file_out, mode='w', index=False, encoding="utf_8_sig")

    def guaidian_stock_append(self, df):
        need_header = False
        file_out = os.path.join(self.root_dir, 'guaidian_stock.csv')
        if not os.path.exists(file_out):
            need_header = True
        cols = ['days', 'ts_code', 'e_days', 'percent', 'cnt_points']
        df.to_csv(file_out, columns=cols, mode='a', header=need_header, encoding="utf_8_sig")

    def mean_guandian_one_stock(self, ts_code, days, ch):
        st_code = ts_code.split('.')[0]
        file_stock = os.path.join(self.stocks_dir, st_code + '.csv')

        # cols = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol', 'amount']
        cols = ['trade_date', 'close', 'vol', 'amount']
        df = pd.read_csv(file_stock, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        if df is None:
            return False

        if df.shape[0] < (360+days):
            #print "%s's data is wrong %d" % (ts_code, df.shape[0])
            return False

        df1 = df['close'][-360-days+1:].reset_index()

        v_a = df1['close'][-361:].values
        max_idx = len(v_a)
        m_a = []
        for i in range(360):
            m_a.append(df1['close'][i:i+days].mean())

        v = np.array(m_a)

        #print
        #index = signal.argrelextrema(v, np.less_equal)

        top_idx = signal.argrelextrema(v, np.greater_equal)
        bottom_idx = signal.argrelextrema(v, np.less_equal)

        top_idx_len = len(top_idx[0])
        bottom_idx_len = len(bottom_idx[0])

        if ch == 0:
            if bottom_idx[0][bottom_idx_len - 1] > top_idx[0][top_idx_len - 1]:
                return False

            up_days = top_idx[0][top_idx_len - 1] - bottom_idx[0][bottom_idx_len - 1]
            d_days = days/10
            if (up_days < d_days) or (up_days > d_days + 2):
                return False
        else:
            stock_eva = [[0, 0] for i in range(20)]
            eva_cnts = len(bottom_idx[0])
            for idx in bottom_idx[0]:
                for i in range(1, 20):
                    if (idx + i) < max_idx:
                        v_n = v_a[idx+i]
                        if v_n > v_a[idx] * 1.05:
                            self.eva[i][0] += 1
                            stock_eva[i][0] += 1
                        else:
                            self.eva[i][1] += 1
                            stock_eva[i][1] += 1

            record = 0
            tmp_i = 1
            for eva1 in stock_eva:
                if eva1[0] > eva1[1]:
                    v = float(eva1[0]) / float(eva1[0] + eva1[1]) * 100
                    if v > 80:
                        record = 1
                        print ts_code, tmp_i, "%.1f%%" % v
                        self.guaidian_stock_append(pd.DataFrame({
                            'days': [days],
                            'ts_code': [ts_code],
                            'e_days': [tmp_i],
                            'percent': [v],
                            'cnt_points': [eva_cnts]}))
                tmp_i += 1

            if record == 1:
                self.stock_list.append(ts_code)

        '''
        print ts_code, up_days, d_days
        print m_a[-7:]
        print v_a[-7:]
        print bottom_idx[0][-5:]
        print top_idx[0][-5:]
        '''

        return True

    def mean_guaidian_filter(self, days, ch):
        print "\n---%d---" % days
        for eva in self.eva:
            eva[0] = 0
            eva[1] = 0
        result = []
        stocks = self.stUtil.get_all_stocks(type)
        for stock in stocks:
            if self.mean_guandian_one_stock(stock, days, ch):
                result.append(stock)

        if ch:
            i = 1
            for eva in self.eva:
                if (eva[0] + eva[1]) > 0:
                    v = float(eva[0]) / float(eva[0] + eva[1]) * 100
                    if v > 20:
                        print i, "%.1f%%" % v
                i += 1

            print self.stock_list
            return

        file_out = os.path.join(self.zb_dir, 'guaidian_' + self.calc_date + '.csv')
        df_out = pd.DataFrame({'guaidian': result})
        df_out.to_csv(file_out, mode='w', index=False, encoding="utf_8_sig")

    def get_all_guaidian_stocks(self):
        file_in = os.path.join(self.root_dir, 'guaidian_stock.csv')
        if not os.path.exists(file_in):
            return []

        df = pd.read_csv(file_in, header=0, usecols=['ts_code'], encoding='utf-8')
        l1 = df.values.flatten()

        return list(set(l1))

    def mean_guaidian_calc(self):
        file_out = os.path.join(self.root_dir, 'guaidian_stock.csv')
        if os.path.exists(file_out):
            os.remove(file_out)
        self.mean_guaidian_filter(10, 1)
        return
        self.mean_guaidian_filter(20, 1)
        self.mean_guaidian_filter(30, 1)
        self.mean_guaidian_filter(40, 1)
        self.mean_guaidian_filter(50, 1)
        self.mean_guaidian_filter(60, 1)

    def below_days_mean(self, ts_code, days):
        st_code = ts_code.split('.')[0]
        file_stock = os.path.join(self.stocks_dir, st_code + '.csv')

        # cols = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol', 'amount']
        cols = ['trade_date', 'close', 'vol', 'amount']
        df = pd.read_csv(file_stock, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        if df is None:
            return False

        if df.shape[0] < (19 + days):
            print "%s's data is wrong %d" % (ts_code, df.shape[0])
            return False

        df1 = df['close'][-19 - days:].reset_index()

        v_a = df1['close'][-20:].values
        m_a = []
        for i in range(20):
            m_a.append(df1['close'][i:i + days].mean())

        if v_a[19] > m_a[19]:
            return False

        if v_a[18] > m_a[18]:
            return False

        if v_a[17] > m_a[17]:
            return False

        return True

    def below_days_mean_and_guandian(self, mean_days, guaidian_days):
        result = []
        result_2 = []
        stocks = self.stUtil.get_all_stocks(3)
        #i = 0
        for stock in stocks:
            if self.mean_guandian_one_stock(stock, guaidian_days, 0):
                result.append(stock)
                #i += 1
                #if i > 6:
                #    return

        for stock in result:
            if self.below_days_mean(stock, mean_days):
                result_2.append(stock)

        print result_2

    def calc_mean(self, m_r, ts_code, days, num):
        st_code = ts_code.split('.')[0]
        file_stock = os.path.join(self.stocks_dir, st_code + '.csv')

        # cols = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol', 'amount']
        cols = ['trade_date', 'close', 'vol', 'amount']
        df = pd.read_csv(file_stock, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        if df is None:
            return False

        if df.shape[0] < ((num-1)+days):
            print "%s's data is wrong %d" % (ts_code, df.shape[0])
            return False

        df1 = df['close'][0-(num-1)-days:].reset_index()

        for i in range(num):
            m_r.append(df1['close'][i:i+days].mean())

        return True

    def mean_20_exceed_mean_100_one_stock(self, ts_code):
        eval_days = 30
        m_20 = []
        m_100 = []

        if self.calc_mean(m_20, ts_code, 20, eval_days) is False:
            return False

        if self.calc_mean(m_100, ts_code, 100, eval_days) is False:
            return False

        if m_100[eval_days-1] > m_20[eval_days-1]:
            return False

        for i in range(eval_days-1):
            if m_20[i] > m_100[i]:
                return False

        return True

    def mean_20_exceed_mean_100(self):
        f_r = []
        stocks = self.stUtil.get_all_stocks(3)
        for stock in stocks:
            if self.mean_20_exceed_mean_100_one_stock(stock):
                f_r.append(stock)

        print f_r




if __name__ == '__main__':
    zbUtil = ZbUtil('../')
    zbUtil.set_calc_date('20211009')
    # zbUtil.kdj_filter(3)
    # zbUtil.kdj_wk_filter(3)
    #zbUtil.mean_20_filter()
    #zbUtil.mean_filter(100)
    #zbUtil.mean_guaidian_calc()
    #zbUtil.get_all_guaidian_stocks()
    #zbUtil.below_days_mean_and_guandian(100, 20)
    zbUtil.mean_20_exceed_mean_100()






