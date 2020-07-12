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

        if v_20[17] < v_20[16]:
            return False

        for i in range(16, 20):
            if v_20[i] < m_20[i]:
                return False

        for i in range(11, 16):
            if v_20[i] > m_20[i]:
                return False

        '''
        for i in range(16):
            if v_20[i] > m_20[i]:
                return False
        '''
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

    def guaidian_one_stock(self, ts_code):
        st_code = ts_code.split('.')[0]
        file_stock = os.path.join(self.stocks_dir, st_code + '.csv')

        # cols = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol', 'amount']
        cols = ['trade_date', 'close', 'vol', 'amount']
        df = pd.read_csv(file_stock, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        if df is None:
            return False

        df = df[-360:].reset_index()
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

        if top_idx[0][top_idx_len - 1] - bottom_idx[0][bottom_idx_len - 1] < 3:
            return False

        if bottom_idx[0][bottom_idx_len - 1] - top_idx[0][top_idx_len - 2] < 3:
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


if __name__ == '__main__':
    zbUtil = ZbUtil('../')
    zbUtil.set_calc_date('20200710')
    # zbUtil.kdj_filter(3)
    # zbUtil.kdj_wk_filter(3)
    # zbUtil.mean_20_filter()
    zbUtil.guaidian_filter()
