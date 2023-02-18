# -*-coding:utf-8-*-
from StUtil import StUtil
import GV
import os
import pandas as pd
import numpy as np
import scipy.signal as signal
import pymysql


class PriceNotify:
    def __init__(self):
        self.name = 'PriceNotify'
        self.root_dir = GV.ROOT_DIR
        self.stocks_dir = GV.STOCKS_DIR
        self.stUtil = StUtil()
        self.stock_list_file = GV.STOCK_LIST_ALL

    @staticmethod
    def get_stock_list():
        conn = pymysql.connect(host='218.78.109.52', port=3306, user='root', passwd='guanjianzhi', db='stock', charset='utf8mb4')
        cursor = conn.cursor()
        sql = 'select * from price_notify'
        cursor.execute(sql)
        data = cursor.fetchall()
        conn.close()
        return data

    def get_action(self, ts_code, buy, sale):
        if not isinstance(buy, float):
            return 0
        if not isinstance(sale, float):
            return 0

        st_code = ts_code.split('.')[0]
        file_stock = os.path.join(self.stocks_dir, st_code + '.csv')

        cols = ['trade_date', 'close', 'vol', 'amount']
        df = pd.read_csv(file_stock, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        if df is None:
            return 0

        if df.shape[0] < 1:
            print("%s's data is wrong %d" % (ts_code, df.shape[0]))
            return 0

        v = df[-1:]['close'].values

        if v <= buy:
            return 1
        elif v >= sale:
            return 2

        return 0

    def calc(self):
        buy = []
        sale = []
        rows = self.get_stock_list()
        for row in rows:
            action = self.get_action(row[1], row[2], row[3])
            if action == 1:
                buy.append(row[1])
            elif action == 2:
                sale.append(row[1])

        return buy, sale

