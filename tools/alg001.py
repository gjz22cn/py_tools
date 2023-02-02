# -*-coding:utf-8-*-
from StUtil import StUtil
import GV
import os
import pandas as pd
import numpy as np
import scipy.signal as signal


class Alg001:
    def __init__(self):
        self.name = 'alg001'
        self.root_dir = GV.ROOT_DIR
        self.stocks_dir = GV.STOCKS_DIR
        self.stUtil = StUtil()
        self.stock_list_file = GV.STOCK_LIST_ALL

    def check_per_stock(self, ts_code):
        st_code = ts_code.split('.')[0]
        file_stock = os.path.join(self.stocks_dir, st_code + '.csv')

        # cols = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol', 'amount']
        cols = ['trade_date', 'low', 'high', 'close']
        df = pd.read_csv(file_stock, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        if df is None:
            return False

        if df.shape[0] < 100:
            print(ts_code, "data is wrong ", df.shape[0])
            return False

        window = 30
        df = df[-window:]
        v = df['close'].values
        cnt = 0
        for i in range(window-1):
            if v[i+1] < v[i]:
                cnt += 1

        if cnt > 0.65 * window:
            return True
        else:
            return False

    def get_match_stocks(self):
        result = []
        stocks = self.stUtil.get_all_stocks(3)
        for stock in stocks:
            if self.check_per_stock(stock):
                result.append(stock)

        return result