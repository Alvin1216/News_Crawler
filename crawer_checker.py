import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
import dateutil.parser
import sys
import pymysql
import datetime
from time import sleep

topic = '健康'

today = datetime.date.today()
oneday = datetime.timedelta(days=1)
yesterday = today - oneday
nextday = today + oneday+ oneday

conn = pymysql.connect(host=ip, user=user, passwd=passwd, db=db, charset="utf8")
cur = conn.cursor()

#SELECT news_url FROM newslist WHERE created_at >='2019-11-27' AND created_at <'2019-11-29' AND news_topic='健康'
try:
    sql = "SELECT news_url FROM newslist WHERE created_at >='"+str(yesterday)+"' AND created_at <'"+str(nextday)+"' AND news_topic='"+str(topic)+"\'" 
    print(sql)
    cur.execute(sql)
    result = cur.fetchall()
except Exception :print("發生異常")

link_list = set()
for one_url in result:
    link_list.add(one_url[0])

new_data_list = []
if(len(link_list)>0):
    for data in data_list:
        if data[4] not in  link_list:
            new_data_list.append(data)

return new_data_list
