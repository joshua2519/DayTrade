#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import json
import csv
import time
import threading
import datetime

from datetime import date

import requests

class CrawlerController(object):
    '''Split targets into several Crawler, avoid request url too long'''

    def __init__(self, targets, max_stock_per_crawler=50):
        self.crawlers = []

        for index in range(0, len(targets), max_stock_per_crawler):
            crawler = Crawler(targets[index:index + max_stock_per_crawler])
            self.crawlers.append(crawler)

    def run(self):
        data = []
        for crawler in self.crawlers:
            data.extend(crawler.get_data())
        return data

class Crawler(object):
    '''Request to Market Information System'''
    def __init__(self, targets):
        endpoint = 'http://mis.twse.com.tw/stock/api/getStockInfo.jsp'
        # Add 1000 seconds for prevent time inaccuracy
        timestamp = int(time.time() * 1000 + 1000000)
        channels = '|'.join('{}.tw'.format(target) for target in targets)
        self.query_url = '{}?_={}&ex_ch={}'.format(endpoint, timestamp, channels)

    def get_data(self):
        try:
            # Get original page to get session
            req = requests.session()
            req.get('http://mis.twse.com.tw/stock/index.jsp',
                    headers={'Accept-Language': 'zh-TW'})

            response = req.get(self.query_url)
            content = json.loads(response.text)
        except Exception as err:
            print(err)
            data = []
        else:
            data = content['msgArray']

        return data

class Recorder(object):
    '''Record data to csv'''
    def __init__(self, path='data'):
        self.folder_path = '{}/{}'.format(path, date.today().strftime('%Y%m%d'))
        if not os.path.isdir(self.folder_path):
            os.mkdir(self.folder_path)

    def record_to_csv(self, data):
        for row in data:
            try:
                file_path = '{}/{}.csv'.format(self.folder_path, row['c'])
                with open(file_path, 'a') as output_file:
                    writer = csv.writer(output_file, delimiter=',')
                    max_sell=row['a'].split('_')[0]
                    max_buy=row['b'].split('_')[0]
                    BorS="";
                    if row['z'] >= max_sell:
                        BorS="r"
                    else:
                        BorS="g"
                    writer.writerow([
                        row['tlong'],# epoch
						row['t'],# 資料時間
                        row['z'],# 最近成交價
                        row['tv'],# 當盤成交量
                        row['v'],# 當日累計成交量
                        row['a'],# 揭示賣價
                        row['f'],# 揭示賣量
                        row['b'],# 揭示買價
                        row['g'],# 揭示買量
						row['o'],# 開盤價
                        BorS # r= 外盤成交，g=內盤成交
                    ])

            except Exception as err:
                print(err)

def main():
    targets = [_.strip() for _ in open('target.csv', 'r')]
    controller = CrawlerController(targets)
    data = controller.run()

    recorder = Recorder()
    recorder.record_to_csv(data)

if __name__ == '__main__':
    #main()    
    while True:
        now = datetime.datetime.now()
        if now.hour >=9:
            threading.Thread(target = main, name =  'getdaytrade' ).start()
            time.sleep(3)
            if now.hour >=14 and now.minute >30:
                print(now)
                break
