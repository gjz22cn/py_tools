# -*-coding:utf-8-*-
import GV
import time
from datetime import datetime, date, timedelta
import os
import pandas as pd
import numpy as np
import csv
import scipy.signal as signal
import pymysql
import tushare as ts
from StockInfo import StockInfo


class StockData:
    def __init__(self):
        self.name = 'StockInfo'
        ts.set_token(GV.TUSHARE_TOKEN)
        self.pro = ts.pro_api()
        self.conn = None
        self.cursor = None

    def get_daily_data(self, ts_code, start_date, end_date):
        df = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df

    def dataframe_to_list(self, df):
        dataset = np.array(df)
        datalist = dataset.tolist()
        return datalist

    def mysql_connect(self):
        self.conn = pymysql.connect(host=GV.MYSQL_SERVER, port=3306, user=GV.MYSQL_USER, passwd=GV.MYSQL_PASSWORD,
                               db=GV.DB_NAME, charset='utf8mb4')
        self.cursor = self.conn.cursor()

    def mysql_disconnect(self):
        self.cursor.close()
        self.conn.close()

    @staticmethod
    def create_table(table):
        """
        名称	        类型	    描述
        ts_code	    str	    股票代码
        trade_date	str	    交易日期
        open	    float	开盘价
        high	    float	最高价
        low	        float	最低价
        close	    float	收盘价
        pre_close	float	昨收价
        change	    float	涨跌额
        pct_chg	    float	涨跌幅
        vol	        float	成交量 （手）
        amount	    float	成交额 （千元）
        """
        conn = pymysql.connect(host=GV.MYSQL_SERVER, port=3306, user=GV.MYSQL_USER, passwd=GV.MYSQL_PASSWORD,
                               db=GV.DB_NAME, charset='utf8mb4')
        cursor = conn.cursor()
        #sql_drop_table = "drop table if exists {} ".format(table)
        sql = "create table if not exists {} (`id` INT NOT NULL AUTO_INCREMENT," \
              "`ts_code` VARCHAR(32)," \
              "`trade_date` VARCHAR(32)," \
              "`open` FLOAT," \
              "`high` FLOAT," \
              "`low` FLOAT," \
              "`close` FLOAT," \
              "`pre_close` FLOAT," \
              "`change` FLOAT," \
              "`pct_chg` FLOAT," \
              "`vol` FLOAT," \
              "`amount` FLOAT," \
              "PRIMARY KEY(`id`))".format(table)
        print(sql)

        try:
            #cursor.execute(sql_drop_table)
            cursor.execute(sql)
        except Exception as e:
            print("Error:{}".format(e))

        cursor.close()
        conn.close()

    @staticmethod
    def alter_table(table):
        self.mysql_connect()
        sql = "alter table {} add m5 FLOAT, add m10 FLOAT, add m20 FLOAT, add m30 FLOAT, add m60 FLOAT, " \
              "add m100 FLOAT, add m120 FLOAT, add m180 FLOAT, add m250 FLOAT;".format(table)
        #sql = "alter table {} drop pc7, drop pc8, drop pc9, drop pc10;".format(table)
        print(sql)

        try:
            self.cursor.execute(sql)
        except Exception as e:
            print("Error:{}".format(e))

        self.mysql_disconnect()


    @staticmethod
    def insert_datas(table, values_list):
        keys_list = "ts_code,trade_date,open,high,low,close,pre_close,`change`,pct_chg,vol,amount"
        conn = pymysql.connect(host=GV.MYSQL_SERVER, port=3306, user=GV.MYSQL_USER, passwd=GV.MYSQL_PASSWORD,
                               db=GV.DB_NAME, charset='utf8mb4')
        cursor = conn.cursor()
        sql = "INSERT INTO {} ({}) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(table, keys_list)
        try:
            cursor.executemany(sql, values_list)
            conn.commit()
        except Exception as e:
            print("Error:{}".format(e))
            conn.rollback()
        cursor.close()
        conn.close()

    @staticmethod
    def get_last_update_date(table):
        self.mysql_connect()
        sql = "select trade_date from {} order by trade_date desc limit 1".format(table)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        self.mysql_disconnect()

        if len(data) != 1:
            return None

        return data[0][0]

    def alter_all_stock_tables(self):
        stock_info = StockInfo()
        stocks = stock_info.get_stock_list()
        for item in stocks:
            ts_code = item[0]
            symbol = item[1]
            tbl_name = ts_code.split('.')[1] + symbol
            self.alter_table(tbl_name)

    def download_daily_info_for_all_stocks(self, start_date_input, end_date_input):
        start_date = start_date_input
        end_date = end_date_input

        if start_date is None:
            start_date = GV.START_DATE

        if end_date is None:
            end_date = time.strftime("%Y%m%d", time.localtime())
        stock_info = StockInfo()
        stocks = stock_info.get_stock_list()
        for item in stocks:
            ts_code = item[0]
            symbol = item[1]
            tbl_name = ts_code.split('.')[1] + symbol

            df = self.get_daily_data(ts_code, start_date, end_date)
            df.sort_values("trade_date", inplace=True, ascending=True)
            data = self.dataframe_to_list(df)
            self.create_table(tbl_name)
            self.insert_datas(tbl_name, data)
            time.sleep(0.1)

    def update_daily_info_for_all_stocks(self):
        stock_info = StockInfo()
        stocks = stock_info.get_stock_list()
        for item in stocks:
            ts_code = item[0]
            symbol = item[1]
            tbl_name = ts_code.split('.')[1] + symbol

            last_date = self.get_last_update_date(tbl_name)
            if last_date is None:
                print("update daily info for {} failed!".format(ts_code))
                continue

            last_datetime = datetime.strptime(str(last_date), "%Y%m%d")
            delta = timedelta(days=1)
            start_datetime = last_datetime + delta

            end_datetime = datetime.now()
            day_of_week = datetime.now().weekday()
            if day_of_week > 4:
                end_datetime = end_datetime - timedelta(days=(day_of_week - 4))

            start_date = start_datetime.strftime('%Y%m%d')
            end_date = end_datetime.strftime('%Y%m%d')
            if start_datetime < end_datetime:
                df = self.get_daily_data(ts_code, start_date, end_date)
                df.sort_values("trade_date", inplace=True, ascending=True)
                data = self.dataframe_to_list(df)
                self.insert_datas(tbl_name, data)
                time.sleep(0.1)
            else:
                print("skip {}, S={} E={}".format(ts_code, start_date, end_date))

    @staticmethod
    def calc_pct_chg_level_for_all_stocks():
        stock_info = StockInfo()

        results = []
        self.mysql_connect()

        stocks = stock_info.get_stock_list()
        for item in stocks:
            ts_code = item[0]
            symbol = item[1]
            tbl_name = ts_code.split('.')[1] + symbol
            sql = "select chg,count(*) as cnt from (select round(pct_chg) as chg from {}) as b " \
                  "group by chg order by chg desc".format(tbl_name)
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            item = [ts_code, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
            for row in rows:
                if (row[0] > -11) and (row[0] < 11):
                    item[11 + int(row[0])] = row[1]
            results.append(item)

        self.mysql_disconnect()

        f = open('../pct_chg_level.csv', 'w', newline='')
        writer = csv.writer(f)
        for i in results:
            writer.writerow(i)
        f.close()

    def calc_means_for_all_stocks(self):
        stock_info = StockInfo()
        stocks = stock_info.get_stock_list()

        self.mysql_connect()
        for item in stocks:
            ts_code = item[0]
            symbol = item[1]
            tbl_name = ts_code.split('.')[1] + symbol
            sql = "select id,close from {} order by id".format(tbl_name)
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            datas = np.array(rows)
            closes=datas[:, 1]
            args = []
            #mean = [5, 10, 20, 30, 60, 100, 120, 180, 250]
            for i in range(len(closes)):
                mean = [0, 0, 0, 0, 0, 0, 0, 0, 0, datas[i][0]]
                if i >= 4:
                    mean[0] = closes[i-4:i+1].mean()

                if i >= 9:
                    mean[1] = closes[i-9:i+1].mean()

                if i >= 19:
                    mean[2] = closes[i-19:i+1].mean()

                if i >= 29:
                    mean[3] = closes[i-29:i + 1].mean()

                if i >= 59:
                    mean[4] = closes[i - 59:i + 1].mean()

                if i >= 99:
                    mean[5] = closes[i - 99:i + 1].mean()

                if i >= 119:
                    mean[6] = closes[i - 119:i + 1].mean()

                if i >= 179:
                    mean[7] = closes[i - 179:i + 1].mean()

                if i >= 249:
                    mean[8] = closes[i - 249:i + 1].mean()

                args.append(mean)

            sql = "update {} set m5=%s,m10=%s,m20=%s,m30=%s,m60=%s,m100=%s,m120=%s,m180=%s,m250=%s " \
                  "where id=%s".format(tbl_name)
            try:
                self.cursor.executemany(sql, args)
                self.conn.commit()
            except Exception as e:
                print("Error:{}".format(e))
                self.conn.rollback()

        self.mysql_disconnect()


if __name__ == '__main__':
    stockData = StockData()
    # stockData.download_daily_info_for_all_stocks(None, None)
    # stockData.update_daily_info_for_all_stocks()
    # stockData.calc_pct_chg_level_for_all_stocks()
    # stockData.alter_all_stock_tables()
    stockData.calc_means_for_all_stocks()

