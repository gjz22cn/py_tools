# -*-coding:utf-8-*-
import time
import hmac
import hashlib
import base64
import urllib.parse
import datetime
import requests
import json


class Report:
    def __init__(self):
        self.access_token = 'b6872b1b7994f63905db712afb3a479f966eaac9145828072c24d2556024440f'
        self.secret = 'SEC2875fbe049418a5b5abf0641f0dbe0013ecbc53373cc005d825c8dc218e6bb93'
        self.today = datetime.datetime.now().strftime('%Y-%m-%d')

    def get_url(self):
        timestamp = str(round(time.time() * 1000))
        secret_enc = self.secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, self.secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        url = f'https://oapi.dingtalk.com/robot/send?access_token={self.access_token}&timestamp={timestamp}&sign={sign}'
        return url

    def send_report_single(self, type_str, stocks):
        print(type_str, stocks)
        num = len(stocks)
        if num < 1:
            return

        url = self.get_url()
        text_str = "#### **" + self.today + " " + type_str + "**\n"

        i = 1
        for stock in stocks:
            if int(i % 2) == 1:
                text_str = text_str + "- " + stock
            elif int(i % 2) == 0:
                text_str = text_str + " " + stock + "\n"
            i = i + 1

        header = {
            "Content-Type": "application/json",
            "Charset": "UTF-8"
        }
        data = {
            "msgtype": "markdown",
            "markdown": {"title": "DailyReport",
                         "text": text_str
                         },
            "at": {
                "isAtAll": False
            }
        }

        r = requests.post(url=url, headers=header, data=json.dumps(data))
        r_status = json.loads(r.text)
        if r_status['errcode'] == 0:
            return True

        return False

    def send_report_list(self, reports):
        url = self.get_url()
        text_str = "## **---" + self.today + "---**\n"

        for item in reports:
            print(item[0], item[1])
            type_str = item[0]
            stocks = item[1]
            num = len(stocks)
            if num < 1:
                continue

            text_str = text_str + "#### **" + type_str + "**\n"

            i = 1
            flag = 0
            for stock in stocks:
                if int(i % 2) == 1:
                    text_str = text_str + "- " + stock
                    flag = 1
                elif int(i % 2) == 0:
                    text_str = text_str + " " + stock + "\n"
                    flag = 0
                i = i + 1

            if flag == 1:
                text_str = text_str + "\n"

        header = {
            "Content-Type": "application/json",
            "Charset": "UTF-8"
        }
        data = {
            "msgtype": "markdown",
            "markdown": {"title": "DailyReport",
                         "text": text_str
                         },
            "at": {
                "isAtAll": False
            }
        }

        r = requests.post(url=url, headers=header, data=json.dumps(data))
        r_status = json.loads(r.text)
        if r_status['errcode'] == 0:
            return True

        return False
