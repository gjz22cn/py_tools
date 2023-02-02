import os
import pandas as pd
import tushare as ts
from DownloadClient import DownloadClient
import GV


class StUtil:
    def __init__(self):
        self.root_dir = GV.ROOT_DIR
        self.stocks_dir = GV.STOCKS_DIR
        self.stock_dir = os.path.join(self.root_dir, 'stock')
        self.stock_today_dir = os.path.join(self.root_dir, 'stock')
        self.p230_dir = os.path.join(self.root_dir, 'p230')
        self.single_data_dir = os.path.join(self.root_dir, 'single_d')
        self.start_date = '20140101'
        self.end_date = '20181130'
        self.pro = ts.pro_api()
        self.downloadClient = DownloadClient()
        self.step_len = 10
        ts.set_token('b1de6890364825a4b7b2d227b64c09a486239daf67451c5638404c62')

    def get_all_stocks(self, type, filename='stock_list.csv'):
        file_path = os.path.join(self.root_dir, filename)
        if not os.path.exists(file_path):
            return []

        data = pd.read_csv(file_path, header=0, usecols=['ts_code'], encoding='utf-8')
        data = data.values.flatten()

        output = []
        if type == 1:
            for v in data:
                if v.startswith('002') or v.startswith('300'):
                    output.append(v)
        elif type == 2:
            for v in data:
                if v.startswith('002') or v.startswith('300'):
                    continue
                output.append(v)
        else:
            output = data

        return output

    def get_all_stocks_by_filename(self, filename='stock_list_tmp.csv'):
        file_path = os.path.join(self.root_dir, filename)
        if not os.path.exists(file_path):
            return []

        data = pd.read_csv(file_path, header=0, usecols=['ts_code'], encoding='utf-8')
        output = data.values.flatten()

        return output

    def get_stock_data_for_backtrader(self, ts_code):
        st_code = ts_code.split('.')[0]
        file_stock = os.path.join(self.stocks_dir, st_code + '.csv')

        cols = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol', 'amount']
        data = pd.read_csv(file_stock, header=0, usecols=cols, dtype={'trade_date': str}, encoding='utf-8')

        data['trade_date'] = pd.to_datetime(data['trade_date'])
        data = data.sort_values(by='trade_date')
        data.index = data['trade_date']
        data['openinterest'] = 0
        data['volume'] = data['vol']
        data = data[
            ['open', 'close', 'high', 'low', 'volume']
        ]
        return data