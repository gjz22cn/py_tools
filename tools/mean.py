# -*-coding:utf-8-*-
from StUtil import StUtil
import GV
import os
import pandas as pd
import numpy as np
import scipy.signal as signal


class Mean:
    def __init__(self):
        self.name = 'Mean'
        self.root_dir = GV.ROOT_DIR
        self.stocks_dir = GV.STOCKS_DIR
        self.stUtil = StUtil()
        self.stock_list_file = GV.STOCK_LIST_ALL

    def below_days_mean(self, ts_code, days):
        st_code = ts_code.split('.')[0]
        file_stock = os.path.join(self.stocks_dir, st_code + '.csv')

        # cols = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol', 'amount']
        cols = ['trade_date', 'close', 'vol', 'amount']
        df = pd.read_csv(file_stock, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        if df is None:
            return False

        if df.shape[0] < (19 + days):
            print(st_code, df.shape[0], "data is wrong")
            return False

        df1 = df['close'][-19 - days:].reset_index()
        v_a = df1['close'][-20:].values
        m_a = []
        for i in range(20):
            m_a.append(df1['close'][i:i + days].mean())

        for i in range(30):
            if v_a[19-i] > m_a[19-i]:
                return False

        return True

    def mean_inflection_one_stock(self, ts_code, days):
        st_code = ts_code.split('.')[0]
        file_stock = os.path.join(self.stocks_dir, st_code + '.csv')
        if not os.path.exists(file_stock):
            print(file_stock + " is missing")
            return False

        # cols = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol', 'amount']
        cols = ['trade_date', 'close', 'vol', 'amount']
        df = pd.read_csv(file_stock, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        if df is None:
            return False

        if df.shape[0] < (360 + days):
            # print "%s's data is wrong %d" % (ts_code, df.shape[0])
            return False

        df1 = df['close'][-360 - days + 1:].reset_index()

        m_a = []
        for i in range(360):
            m_a.append(df1['close'][i:i + days].mean())

        v = np.array(m_a)

        top_idx = signal.argrelextrema(v, np.greater_equal)
        bottom_idx = signal.argrelextrema(v, np.less_equal)

        top_idx_len = len(top_idx[0])
        bottom_idx_len = len(bottom_idx[0])

        if bottom_idx[0][bottom_idx_len - 1] > top_idx[0][top_idx_len - 1]:
            return False

        up_days = top_idx[0][top_idx_len - 1] - bottom_idx[0][bottom_idx_len - 1]
        if up_days < 3:
            return False

        return True

    def below_days_mean_and_inflection(self, mean_days, mean_inflection_days):
        result = []
        result_2 = []
        stocks = self.stUtil.get_all_stocks(3)
        for stock in stocks:
            if self.mean_inflection_one_stock(stock, mean_inflection_days):
                result.append(stock)

        for stock in result:
            if self.below_days_mean(stock, mean_days):
                result_2.append(stock)

        return result_2

    def mean_inflection_with_input_stocks(self, stocks, mean_inflection_days):
        result = []
        for stock in stocks:
            if self.mean_inflection_one_stock(stock, mean_inflection_days):
                result.append(stock)

        return result

    def below_days_mean_with_input_stocks(self, input_stocks, mean_days):
        result = []
        stocks = input_stocks
        if input_stocks is None:
            stocks = self.stUtil.get_all_stocks(3)

        for stock in stocks:
            if self.below_days_mean(stock, mean_days):
                result.append(stock)

        return result
