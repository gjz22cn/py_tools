# -*-coding:utf-8-*-
import GV
import os
import pandas as pd
import numpy as np
import scipy.signal as signal
import pymysql
import tushare as ts


class StockInfo:
    def __init__(self):
        self.name = 'StockInfo'
        ts.set_token(GV.TUSHARE_TOKEN)
        self.pro = ts.pro_api()

    def get_stock_list(self):
        conn = pymysql.connect(host=GV.MYSQL_SERVER, port=3306, user=GV.MYSQL_USER, passwd=GV.MYSQL_PASSWORD,
                               db=GV.DB_NAME, charset='utf8mb4')
        cursor = conn.cursor()
        sql = 'select ts_code,symbol from ' + GV.MYSQL_STOCK_INFO_TBL
        cursor.execute(sql)
        data = cursor.fetchall()
        conn.close()
        return data

    def dataframe_to_list(self, df):
        dataset = np.array(df)
        datalist = dataset.tolist()
        return datalist

    @staticmethod
    def create_table(table):
        conn = pymysql.connect(host=GV.MYSQL_SERVER, port=3306, user=GV.MYSQL_USER, passwd=GV.MYSQL_PASSWORD,
                               db=GV.DB_NAME, charset='utf8mb4')
        cursor = conn.cursor()
        #sql_drop_table = "drop table {} ".format(table)
        sql = "create table if not exists {} (`id` INT NOT NULL AUTO_INCREMENT," \
              "`ts_code` VARCHAR(32)," \
              "`symbol` VARCHAR(128)," \
              "`name` VARCHAR(255)," \
              "`area` VARCHAR(255)," \
              "`industry` VARCHAR(255)," \
              "`list_date` VARCHAR(64)," \
              "PRIMARY KEY(`id`))".format(table)

        try:
            #cursor.execute(sql_drop_table)
            cursor.execute(sql)
        except Exception as e:
            print("Error:{}".format(e))

        cursor.close()
        conn.close()

    @staticmethod
    def insert_datas(table, keys_list, values_list):
        conn = pymysql.connect(host=GV.MYSQL_SERVER, port=3306, user=GV.MYSQL_USER, passwd=GV.MYSQL_PASSWORD,
                               db=GV.DB_NAME, charset='utf8mb4')
        cursor = conn.cursor()
        sql = "INSERT INTO {} ({}) VALUES (%s,%s,%s,%s,%s,%s)".format(table, keys_list)
        print(sql)
        try:
            cursor.executemany(sql, values_list)
            conn.commit()
        except Exception as e:
            print("Error:{}".format(e))
            conn.rollback()
        cursor.close()
        conn.close()

    def download_all_stocks(self):
        key_list = "ts_code,symbol,name,area,industry,list_date"
        df = self.pro.query('stock_basic', exchange='', list_status='L', fields=key_list)
        data = self.dataframe_to_list(df)
        self.create_table(GV.MYSQL_STOCK_INFO_TBL)
        self.insert_datas(GV.MYSQL_STOCK_INFO_TBL, key_list, data)


if __name__ == '__main__':
    stockInfo = StockInfo()
    stockInfo.download_all_stocks()




