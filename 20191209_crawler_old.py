# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
import dateutil.parser
import sys
import pymysql
import datetime
from time import sleep



def crawl_news_links(url, news_topic):

    links = []

    if news_topic != 'all' :
        yahoo_r = requests.get(url.format(news_topic))
    else:
        yahoo_r = requests.get(archive)

    yahoo_soup = BeautifulSoup(yahoo_r.text, 'html.parser')

    topic = yahoo_soup.find_all('div', {'class': 'Cf'})

    for info in topic:
        link = ""
        try:
            link = info.find_all('a', href=True)[0]
            if link.get('href') != '#':
                links.append(base_url + link.get("href"))
        except:
            link = None

    print("News count : {}".format(len(links)))  
    
    return links

def news_parser(links):

    data_list = []
    pattern = re.compile(r'<[^>]*>')

    for link in links:
    
        try:
            news = requests.get(link)
            single_news = BeautifulSoup(news.text, 'html.parser')
        except: sleep(10)
        
        try:
            # get news titles ##########################################################
            titles = str(single_news.find_all('h1', {'class':''})[0])
            find_tags = pattern.findall(titles)
            for tag in find_tags:
                titles = titles.replace(tag, '')
         
            # get news contents ##########################################################
            content = single_news.find_all('p')
            
            p_tmp = ''
            for p in content:
                if len(p) == 1 and type(p.contents[0]).__name__ != 'Tag':
                    p = str(p).replace('<p>', '')
                    p = str(p).replace('</p>', '')
                    p_tmp = p_tmp + p
                
            contents = p_tmp
                    
            # get image ulrs ##########################################################
            i_tmp = []
            img_link = single_news.find_all('img', class_ = "caas-img")
            
            for image in img_link:
                i_tmp.append(image['src'])

            if len(i_tmp) > 1:
                images = i_tmp[-1]
            else:
                images = None
            
            if images == '':
                images = None
            
            # image = None => default images to every topics ##########################
            
            if images == None:
                
                if news_topic == 'technology':
                    images = 'https://i.screenshot.net/18rp4t4'
                elif news_topic == 'sports':
                    images = 'https://i.screenshot.net/qlvzpbp'
                elif news_topic == 'finance':
                    images = 'https://i.screenshot.net/n3d8gtk'
                elif news_topic == 'politics':
                    images = 'https://i.screenshot.net/7d12xi2'
                elif news_topic == 'entertainment':
                    images = 'https://i.screenshot.net/kmy96u0'
                elif news_topic == 'health':
                    images = 'https://i.screenshot.net/32o6ziq'
            
            # get news times ##########################################################

            time = single_news.find_all('time')
        
            for t in time:
                d = dateutil.parser.parse(t['datetime'])
                times = d.strftime('%Y-%m-%d %H:%M:%S')
            
            # news_topic -> news_topics_ch[idx]
            
            if contents == '' or contents == '更多 NOWnews 今日新聞報導' or contents == ' 更多 NOWnews 今日新聞報導' or len(contents)<15:
                print("this news has bkank content : "+ str(titles))
                break
        
            data_list.append([news_topics_ch[idx], times, titles, contents, images, link])
        except:
            continue
    
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

def send_to_db(news_topics):
    idx = 0
    for news_topic in news_topics:
        print('News topic : {}'.format(news_topic))
        links = crawl_news_links(url, news_topic)
        data_list = news_parser(links)
        data_list = cheacker_from_db(news_topics_ch[idx], data_list)
        save_to_db(news_topic, data_list)    
        idx = idx + 1
       
if __name__ == "__main__":

    base_url = "https://tw.news.yahoo.com"

    #news_topics =['all_topic', 'technology', 'sports', 'finance', 'politics', 'entertainment', 'society', 'health', 'travel', 'world']
    news_topics =['technology', 'sports', 'finance', 'politics', 'entertainment','health']
    news_topics_ch =['科技', '運動', '財經', '政治', '娛樂', '健康']
    #news_topics =['technology']
    #news_topics_ch =['科技']


    url = "https://tw.news.yahoo.com/{}/archive"
    archive = 'https://tw.news.yahoo.com/archive' #各家新聞
    
    
    print('connect to :' + ip)
    #blank_content_checker_db()
    #send_to_db(news_topics)
    
    idx=0
    for news_topic in news_topics:
        print('News topic : {}'.format(news_topic))
        links = crawl_news_links(url, news_topic)
        data_list = news_parser(links)
        data_list = cheacker_from_db(news_topics_ch[idx], data_list)
        save_to_db(news_topic, data_list)    
        idx = idx + 1
    
