# -*- coding: utf-8 -*-
"""
20200818 modify for revised Yahoo News
Yahoo 新聞改版，爬蟲重寫
Author: alvinhuang
"""

import requests
import re
from bs4 import BeautifulSoup
import dateutil.parser
import sys
import pymysql
import datetime
from time import sleep

# Remember to add DB config

def crawl_news_links(url, news_topic):
    yahoo_r = requests.get(url.format(news_topic))
    yahoo_soup = BeautifulSoup(yahoo_r.text, 'html.parser')
    raw_news = yahoo_soup.find_all('a', {'class': 'Fw(b) Fz(20px) Lh(23px) Fz(17px)--sm1024 Lh(19px)--sm1024 mega-item-header-link Td(n) C(#0078ff):h C(#000) LineClamp(2,46px) LineClamp(2,38px)--sm1024 not-isInStreamVideoEnabled'})
    
    links = []
    for news in raw_news:
        front_url = 'https://tw.news.yahoo.com/'
        links.append(front_url + news['href'])
    
    return links

def crawl_single_news(url,news_topics_ch):
    #lock the link
    link = url

    #Get title of news
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    titles = soup.title.string[:-12]

    #Get img of newsi
    #news_topics_ch =['科技', '運動', '財經', '政治', '娛樂', '健康']
    img = soup.findAll("img", {"class": "Maw(100%)"})
    try:
        images = img[0]['src']
    except:
        images = 'NA'

    if images == 'NA':
        if news_topics_ch == '科技':
            images = 'https://i.screenshot.net/18rp4t4'
        elif news_topics_ch == '運動':
            images = 'https://i.screenshot.net/qlvzpbp'
        elif news_topics_ch == '財經':
            images = 'https://i.screenshot.net/n3d8gtk'
        elif news_topics_ch == '政治':
            images = 'https://i.screenshot.net/7d12xi2'
        elif news_topics_ch == '娛樂':
            images = 'https://i.screenshot.net/kmy96u0'
        elif news_topics_ch == '健康':
            images = 'https://i.screenshot.net/32o6ziq'

    #Get time of news
    time = soup.find("time")
    d = dateutil.parser.parse(time['datetime'])
    times = d.strftime('%Y-%m-%d %H:%M:%S')

    #Get content of news
    paragraphs = soup.findAll("p", {"class": "canvas-atom canvas-text Mb(1.0em) Mb(0)--sm Mt(0.8em)--sm"})
    contents = ''
    for div in paragraphs:
        paragraph = div['content']
        if 'href' not in paragraph and '&nbsp;' not in paragraph and '<br>' not in paragraph and 'span' not in paragraph and '</strong>' not in paragraph :
            contents = contents + paragraph
            
    single_news = [news_topics_ch, times, titles, contents, images, link]
    print(single_news)
    print("Take a rest!")
    sleep(3)
    return single_news


def crawl_set_news(urls,news_topic_ch):
    data_list = []
    for url in urls:
        print(url,news_topic_ch)
        try:
            data = crawl_single_news(url,news_topic_ch)
        except Exception as e:
            print(e)
            continue
        
        if len(data[3]) > 10:
            data_list.append(data)
        else:
            print('Empty content!')
            
    return data_list

def save_to_db(news_topic, data_list):
    conn = pymysql.connect(host=ip, user=user, passwd=passwd, db=db, charset="utf8")
    cur = conn.cursor()                              
    
    datetime_object = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(datetime_object)

    if(len(data_list)==0):
        print("repeat !!!")
    else:
        for data in data_list: # (0:news_topic, 1:times, 2:titles, 3:contents, 4:images, 5:link)
            try:
                cur.execute("INSERT INTO newslist (news_title, news_content, news_picture, news_url, news_topic, news_info, news_date, created_at, updated_at) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (data[2], data[3], data[4], data[5], data[0], data[1], data[1], datetime_object, datetime_object))
            except Exception :print("發生異常 in insert data to db")
    cur.close()                                   
    conn.commit()                                  
    conn.close()       
    print("Save to DB successfully !!!")

#check whether has repeated news in db
def cheacker_from_db(topic,data_list):
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    nextday = today + oneday+ oneday

    conn = pymysql.connect(host=ip, user=user, passwd=passwd, db=db, charset="utf8")
    cur = conn.cursor()

    try:
        sql = "SELECT news_url FROM newslist WHERE created_at >='"+str(yesterday)+"' AND created_at <'"+str(nextday)+"' AND news_topic='"+str(topic)+"\'" 
        print(sql)
        cur.execute(sql)
        result = cur.fetchall()
    except Exception :print("發生異常 in select from db")

    link_list = list()
    for one_url in result:
        link_list.append(one_url[0])

    if len(link_list) == 0:
        #表示裡面所有的新聞都沒有 所以要加入新的新聞
        print("All news are new one!")
        return data_list
    else:
        new_data_list = []
        if(len(link_list)>0):
            for data in data_list:
                if data[5] not in  link_list:
                    print("add_new_news: " + data[2])
                    new_data_list.append(data)
                #else:
                #    print("repeat: in now data "+data[5])
            return new_data_list

#check whether has blank news in db
def blank_content_checker_db():

    conn = pymysql.connect(host=ip, user=user, passwd=passwd, db=db, charset="utf8")
    cur = conn.cursor()

    print('load data from db!')
    try:
        sql = "SELECT news_content,news_id FROM newslist" 
        print(sql)
        cur.execute(sql)
        result = cur.fetchall()
        print('load data finished!')
    except Exception :print("發生異常 in select from db")
    
    need_to_del=[]    
    for one_news in result:
        print(str(one_news[1])+" : "+str(len(one_news[0])))
        if(len(one_news[0])<15):
            need_to_del.append(one_news[1])
    print("need to delete: "+str(need_to_del))
    
if __name__ == "__main__":
    base_url = "https://tw.news.yahoo.com"

    news_topics =['technology', 'sports', 'finance', 'politics', 'entertainment','health']
    news_topics_ch =['科技', '運動', '財經', '政治', '娛樂', '健康']

    url = "https://tw.news.yahoo.com/{}/archive"
    archive = 'https://tw.news.yahoo.com/archive'


    print('connect to :' + ip)

    for index,news_topic in enumerate(news_topics):
        print('News topic : {}'.format(news_topic))
        links = crawl_news_links(url, news_topic)
        print(len(links))
        data_list = crawl_set_news(links,news_topics_ch[index])
        data_list = cheacker_from_db(news_topics_ch[index], data_list)
        print(data_list)
        save_to_db(news_topic, data_list)
        print('Take a rest!!')
        sleep(10)