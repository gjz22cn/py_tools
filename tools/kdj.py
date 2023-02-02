# -*-coding:utf-8-*-
from StUtil import StUtil
import GV
import os
import pandas as pd
import numpy as np
import scipy.signal as signal


class Kdj:
    def __init__(self):
        self.name = 'Kdj'
        self.root_dir = GV.ROOT_DIR
        self.stocks_dir = GV.STOCKS_DIR
        self.stUtil = StUtil(self.root_dir)
        self.stock_list_file = GV.STOCK_LIST_ALL

    def calc_kdj(self, ts_code):
        st_code = ts_code.split('.')[0]
        file_stock = os.path.join(self.stocks_dir, st_code + '.csv')

        # cols = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol', 'amount']
        cols = ['trade_date', 'low', 'high', 'close']
        df = pd.read_csv(file_stock, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        if df is None:
            return None,None,None

        if df.shape[0] < 30:
            print("%s's data is wrong %d") % (ts_code, df.shape[0])
            return None,None,None

        df = df[-30:]
        # 9 3 3
        low_list = df['low'].rolling(window=9).min()
        low_list.fillna(value=df['low'].expanding().min(), inplace=True)
        high_list = df['high'].rolling(window=9).max()
        high_list.fillna(value=df['high'].expanding().max(), inplace=True)
        rsv = (df['close'] - low_list) / (high_list - low_list) * 100
        df['KDJ_K'] = rsv.ewm(adjust=False, alpha=1 / 3).mean()
        # pd.Series.ewm(rsv, com=2).mean()
        df['KDJ_D'] = df['KDJ_K'].ewm(adjust=False, alpha=1 / 3).mean()
        # pd.Series.ewm(stock_datas['K'], com=2).mean()
        df['KDJ_J'] = 3 * df['KDJ_K'] - 2 * df['KDJ_D']

        return df[-1:].values[0][-3:]

    def kdj_j_below(self, input_stocks, j):
        result = []
        stocks = input_stocks
        if input_stocks is None:
            stocks = self.stUtil.get_all_stocks(3)
        for stock in stocks:
            _, _, j_v = self.calc_kdj(stock)
            if j is not None:
                if j_v < j:
                    result.append(stock)

        return result
