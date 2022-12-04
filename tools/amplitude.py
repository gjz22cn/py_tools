# -*-coding:utf-8-*-
from operator import itemgetter

from StUtil import StUtil
import GV
import os
import pandas as pd
import numpy as np


class Amplitude:
    def __init__(self):
        self.name = 'Amplitude'
        self.root_dir = GV.ROOT_DIR
        self.stocks_dir = GV.STOCKS_DIR
        self.stUtil = StUtil(self.root_dir)
        self.stock_list_file = GV.STOCK_LIST_ALL

    def get_exceed_increment_days_num(self, ts_code, during_days, increment):
        st_code = ts_code.split('.')[0]
        file_stock = os.path.join(self.stocks_dir, st_code + '.csv')
        if not os.path.exists(file_stock):
            print(file_stock + " is missing")
            return 0

        # cols = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol', 'amount']
        cols = ['trade_date', 'pct_chg']
        df = pd.read_csv(file_stock, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        if df is None:
            return 0

        if df.shape[0] < during_days:
            # print "%s's data is wrong %d" % (ts_code, df.shape[0])
            return 0

        v_a = df['pct_chg'][-during_days:].values
        num = np.sum(v_a > increment)
        return num

    def getZhangTingNum(self, during_days, num, increment):
        result = []
        cal_num = 0
        stocks = self.stUtil.get_all_stocks(3, filename=self.stock_list_file)
        for stock in stocks:
            cal_num = self.get_exceed_increment_days_num(stock, during_days, increment)
            if cal_num >= num:
                result.append((stock, cal_num))

        result = sorted(result, key=itemgetter(1), reverse=True)
        print(result)

        return result
