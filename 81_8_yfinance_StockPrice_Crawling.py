#!/usr/bin/env python
# coding: utf-8

# In[1]:


stock_list_us = ['SMTC','TXN','TSM','CRUS','SWKS','WOLF','QRVO','TER','ADI','ASML',
                 'ENTG','INTC','ON','MCHP','MPWR','AVGO','MU','SLAB','QCOM','LRCX',
                 'MRVL','NVDA','AMD','AMAT','MKSI','KLAC','NXPI','AAPL','AMZN','CSCO',
                 'DELL','META','GOOG','HPE','HPQ','IBM','MSFT','NTAP','ORCL','STX','WDC','VZ',
                 '^IXIC','^GSPC','^SP500-45','^SOX', 'GC=F']

stock_list_hk = ['0992.HK']

stock_list_kr = {'SEC':'005930',
                 'HYNIX':'000660'}
index_list_kr = {'KOSPI':'1001',
                 'KOSPI200':'1028',
                 'KOSDAQ':'2001'}

start='1990-01-01'
file_nm = '8_STOCKPRICE'
flag_MonthClose = True
mail_flag = True

###################################################

import pandas as pd
import datetime as dt
import yfinance as yf
from pykrx import stock
import numpy as np
import telegram

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.encoders import encode_base64
from email.header import Header
import getpass
import os
from tqdm.notebook import tqdm

dfTTL_us = pd.DataFrame()
for nm in tqdm(stock_list_us, desc='US STOCK PRICE Crawling', leave=True) :
    df = yf.download(nm ,start=start, end=dt.datetime.today().strftime('%Y-%m-%d'), progress=False)[['Adj Close']]
    df = df.rename(columns={'Adj Close':nm})
    dfTTL_us = pd.concat([dfTTL_us,df], axis=1)
    
dfTTL_hk = pd.DataFrame()
for nm in tqdm(stock_list_hk, desc='HK STOCK PRICE Crawling', leave=True) :
    df = yf.download(nm ,start=start, end=dt.datetime.today().strftime('%Y-%m-%d'), progress=False)[['Adj Close']]
    df = df.rename(columns={'Adj Close':nm})
    dfTTL_hk = pd.concat([dfTTL_hk,df], axis=1)
    

dfTTL_kr = pd.DataFrame()
for nm, tk in tqdm(stock_list_kr.items(), desc='KR STOCK PRICE Crawling', leave=True) :
    df = stock.get_market_ohlcv_by_date(start, dt.datetime.today().strftime('%Y%m%d'), tk)[['종가']]
    df = df.rename(columns={'종가':nm})
    dfTTL_kr = pd.concat([dfTTL_kr,df], axis=1)

for nm, tk in tqdm(index_list_kr.items(), desc='KR INDEX Crawling', leave=True) :
    df = stock.get_index_ohlcv_by_date(start, dt.datetime.today().strftime('%Y%m%d'), tk)[['종가']]
    df = df.rename(columns={'종가':nm})
    dfTTL_kr = pd.concat([dfTTL_kr,df], axis=1) 

dfTTL_kr.index.name = 'Date'

if flag_MonthClose:
    dfTTL_us.sort_index(ascending=True, inplace=True)
    dfTTL_us['YEAR'], dfTTL_us['MONTH'] = dfTTL_us.index.year, dfTTL_us.index.month
    dfTTL_us = dfTTL_us.drop_duplicates(['YEAR','MONTH'], keep='last')
    dfTTL_us.drop(columns=['YEAR','MONTH'], inplace=True)
    dfTTL_hk.sort_index(ascending=True, inplace=True)
    dfTTL_hk['YEAR'], dfTTL_hk['MONTH'] = dfTTL_hk.index.year, dfTTL_hk.index.month
    dfTTL_hk = dfTTL_hk.drop_duplicates(['YEAR','MONTH'], keep='last')
    dfTTL_hk.drop(columns=['YEAR','MONTH'], inplace=True)
    dfTTL_kr.sort_index(ascending=True, inplace=True)
    dfTTL_kr['YEAR'], dfTTL_kr['MONTH'] = dfTTL_kr.index.year, dfTTL_kr.index.month
    dfTTL_kr = dfTTL_kr.drop_duplicates(['YEAR','MONTH'], keep='last')
    dfTTL_kr.drop(columns=['YEAR','MONTH'], inplace=True)
    

dfTTL_us.index = dfTTL_us.index.strftime('%Y-%m-%d')
dfTTL_hk.index = dfTTL_hk.index.strftime('%Y-%m-%d')
dfTTL_kr.index = dfTTL_kr.index.strftime('%Y-%m-%d')

dfTTL = pd.concat([dfTTL_us, dfTTL_hk, dfTTL_kr], axis=1)
dfTTL.index = pd.to_datetime(dfTTL.index)

dfTTL.insert(0,'YEAR', dfTTL.index.year)
dfTTL.insert(1,'MONTH', dfTTL.index.month)
dfTTL.insert(2,'DAY', dfTTL.index.day)
dfTTL.insert(3,'UP_DATE', dt.datetime.today().strftime('%Y-%m-%d'))
dfTTL.sort_values(by=['YEAR','MONTH','DAY'], inplace=True)
dfTTL.reset_index(drop=True, inplace=True)

dfTTL = dfTTL[['YEAR', 'MONTH', 'DAY', 'UP_DATE', 'SMTC', 'TXN', 'TSM', 'CRUS', 'SWKS',
               'WOLF', 'QRVO', 'TER', 'ADI', 'ASML', 'ENTG', 'INTC', 'ON', 'MCHP',
               'MPWR', 'AVGO', 'MU', 'SLAB', 'QCOM', 'LRCX', 'MRVL', 'NVDA', 'AMD',
               'AMAT', 'MKSI', 'KLAC', 'NXPI', 'AAPL', 'AMZN', 'CSCO', 'DELL', 'META',
               'GOOG', 'HPE', 'HPQ', 'IBM', 'MSFT', 'NTAP', 'ORCL', 'STX', 'WDC', 'VZ',
               '^IXIC', '^GSPC', '^SP500-45', '^SOX', 'GC=F',
               '0992.HK',
               'SEC', 'HYNIX', 'KOSPI', 'KOSPI200', 'KOSDAQ']]
dfTTL.columns = [nm.replace("^","") if "^" in nm else nm for nm in dfTTL.columns]
dfTTL.columns = [nm.replace("=","_") if "=" in nm else nm for nm in dfTTL.columns]
dfTTL.columns = [nm.replace(".","_") if "." in nm else nm for nm in dfTTL.columns]
dfTTL.columns = [nm.replace("-","_") if "-" in nm else nm for nm in dfTTL.columns]
dfTTL.to_csv(f'./{file_nm}.csv')

# mail 
if mail_flag :
    print('\npreparing mail\n')
    msg = MIMEMultipart()
    msg['From'] = 'iamsukwon@naver.com'
    msg['To'] = 'sw46.hong@samsung.com, yesl.jo@samsung.com'
    msg['Subject'] = Header(s=f'{file_nm}_Crawling', charset='utf-8')
    body = MIMEText('첨부된 파일을 확인해 주세요.', _charset='utf-8')
    msg.attach(body)

    files = [f'./{file_nm}.csv']

    for file in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(file,"rb").read())
        encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(file))
        msg.attach(part)

    mailServer = smtplib.SMTP('smtp.naver.com', 587)
    mailServer.starttls()
    mailServer.login('iamsukwon@naver.com', 'Okok7410!!')
    mailServer.send_message(msg)
    mailServer.quit()
    print('Done')
    bot = telegram.Bot(token='5654356034:AAE6al7LcrJEnF_D-mAhXAxV2_dfGtQ17a4')
    chat_id = 47418377
    bot.sendMessage(chat_id=chat_id, text=f"{file_nm} Done")


# In[ ]:




