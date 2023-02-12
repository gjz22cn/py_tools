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

    def send_report(self, type_str, stocks):
        url = self.get_url()
        header = {
            "Content-Type": "application/json",
            "Charset": "UTF-8"
        }
        data = {
            "msgtype": "markdown",
            "markdown": {"title": "DailyReport",
                         "text": {
                             "Type": type_str,
                             "Date": self.today,
                             "Members": stocks
                         }
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