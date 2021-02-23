from mysql import connector
from datetime import datetime
from datetime import timedelta
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import os
import time as t
import urllib
import numpy as np 
import json
import pytz
from pytz import timezone
import logging
import matplotlib.pyplot as plt
import base64
import io
import binascii
from matplotlib import rcParams
from test import debug as masterdebug
import stash_reader
import math
import error_handler
import six
import matplotlib as mpl
import re
import sys
rcParams.update({'figure.autolayout': True})
debug=masterdebug
logging.basicConfig(level=logging.INFO)

if debug == True:
    with open('pass.txt') as f:
        lines = f.readlines()
        f.close()
        env='local_laptop'
elif debug ==False:
    with open(r'/app/secrets/credentials') as f:
        lines = f.readlines()
        f.close()
        env=os.getenv('ENVVAR3')
 
#MOS db connection information
localhost_mos=str(lines[0])
localhost_mos=str.strip(localhost_mos)
localhost_mos_rpt2=str(lines[1])
localhost_mos_rtp2=str.strip(localhost_mos_rpt2)
user_mos=str(lines[2])
user_mos=str.strip(user_mos)
passwd_mos=str(lines[3])
passwd_mos=str.strip(passwd_mos)
database_mos=str(lines[4]) 
database_mos=str.strip(database_mos)
#MOS db connection information

#Teams webhooks
if env=='local_laptop' or env=='dev':
    teams_webhook=str(lines[6]) 
    teams_webhook=str.strip(teams_webhook)
    teams_webhook_45=str(lines[9]) 
    teams_webhook_45=str.strip(teams_webhook_45)
    teams_webhook_Z4=str(lines[6]) 
    teams_webhook_Z4=str.strip(teams_webhook_Z4)
    teams_webhook_Z3=teams_webhook_Z4
elif env=='prod':
    teams_webhook=str(lines[7]) 
    teams_webhook=str.strip(teams_webhook)
    teams_webhook_45=str(lines[8]) 
    teams_webhook_45=str.strip(teams_webhook_45)
    teams_webhook_Z4=str(lines[10]) 
    teams_webhook_Z4=str.strip(teams_webhook_Z4)
    teams_webhook_Z3=str(lines[11]) 
    teams_webhook_Z3=str.strip(teams_webhook_Z3)

def logger (device,time,env,state):
    t1='{:%Y-%m-%d %H:%M:%S}'.format(time)
    dictionary={
        'device':device,
        'time':t1,
        'env':env,
        'state':state 
    }
    j=json.dumps(dictionary)
    #print (j)
    logging.info(j)

def db_connector(write,db,**kwargs): #db=MOS,PLC,Pallet,prodengdb kwargs=**sql,**schema,**dataframe,**append,**table_name,**includeIndex,**returnInsertId
    logger('db_connector',datetime.utcnow(),env,'Running')
    now=datetime.utcnow()
    df=pd.DataFrame()
    if db=="MOS" and "sql" in kwargs and write==False:
        sql=kwargs['sql']
        #check for database hostname delay due to multiple issues with MOS db and replication errors causing incarruate reports
        tables=["thingpath","thing"]
        replication_error=False
        rpt2=False #health check on rpt2 also
        df=pd.DataFrame()
        table_lag=[]
        for table in tables:
            sql2=f"SHOW TABLE STATUS FROM sparq LIKE '{table}';"
            try:
                engine=create_engine(f"mysql+pymysql://{user_mos}:{passwd_mos}@{localhost_mos}:3307/{database_mos}")
                con_db=engine.connect()
                df=pd.read_sql(sql2,con=con_db)
                con_db.close()
            except Exception as e:
                print (e)
                error_handler.e_handler(e)
                con_db.close()
            if (now-df['Update_time'][0])>timedelta(minutes=5) or len(df.index)==0:
                #delayed by 5 min to current time, try rpt2
                replication_error=True
                try:
                    engine=create_engine(f"mysql+pymysql://{user_mos}:{passwd_mos}@{localhost_mos_rtp2}:3307/{database_mos}")
                    con_db=engine.connect()
                    df=pd.read_sql(sql2,con=con_db)
                    con_db.close()
                except Exception as e:
                    print (e)
                    error_handler.e_handler(e)
                    con_db.close()
                if (now-df['Update_time'][0])>timedelta(minutes=5) and len(df.index)==0:
                    rpt2=False
                elif (now-df['Update_time'][0])<timedelta(minutes=5) and len(df.index)>0:
                    rpt2=True
        if replication_error==False:
            try:
                engine=create_engine(f"mysql+pymysql://{user_mos}:{passwd_mos}@{localhost_mos}:3307/{database_mos}")
                con_db=engine.connect()
                df=pd.read_sql(sql,con=con_db)
                con_db.close()
            except Exception as e:
                print (eval)
                error_handler.e_handler(e)
                con_db.close()
        elif replication_error==True and rpt2==True:
            try:
                engine=create_engine(f"mysql+pymysql://{user_mos}:{passwd_mos}@{localhost_mos_rpt2}:3307/{database_mos}")
                con_db=engine.connect()
                df=pd.read_sql(sql,con=con_db)
                con_db.close()
            except Exception as e:
                print (e)
                error_handler.e_handler(e)
                con_db.close()
    return df


def days_hours_minutes(td):
    days=td.days
    hours=td.seconds//3600
    m,s= divmod(td.seconds, 60)
    minutes=m+(s/60)
    return days,hours,minutes

def precision(input):
    #logger('precision',datetime.utcnow(),env,'Running')
    return float('%.2f'%(input))

def reporter (device,timestamp):
    logger('reporter',datetime.utcnow(),env,'Running')
    dtdf=pd.DataFrame()
    d=[device]
    t=[timestamp]
    dtdf.insert(loc=0, column='datetime', value=t)
    dtdf.insert(loc=1, column='device', value=d)
    db_connector(True,"prodengdb",dataframe=dtdf, append=True,table_name='runtime')
    msg=f"succesfully reported heartbeat for {device}"
    logger('reporter',datetime.utcnow(),env,'Ended')
    return print(msg)

def week(now,lookback): #0 current week, 1 previouse week etc.
    logger('week',datetime.utcnow(),env,'Running')
    datetime_pst=pytz.utc.localize(now).astimezone(pytz.timezone('US/Pacific'))
    WW=datetime_pst.isocalendar()[1]-lookback
    WD=datetime_pst.isocalendar()[2]
    WY=datetime_pst.isocalendar()[0]
    d = f'{WY}-W{WW}'
    start_of_week_date=datetime.strptime(d + '-1', '%G-W%V-%u')+timedelta(days=-1)
    logger('week',datetime.utcnow(),env,'Ended')
    return WW,WD,WY,start_of_week_date

def shift(now,now_is_UTC):
    logger('shift',datetime.utcnow(),env,'Running')
    if now_is_UTC==True:
        datetime_pst=pytz.utc.localize(now).astimezone(pytz.timezone('US/Pacific'))
    if now_is_UTC==False:
        datetime_pst=now
    WW=datetime_pst.isocalendar()[1]
    WD=datetime_pst.isocalendar()[2]
    WY=datetime_pst.isocalendar()[0]
    num = WW
    date=datetime_pst.replace(minute=0,second=0,microsecond=0)
    s=""
    hour_of_shift=0
    if (num % 2) == 0:    
        if (WD <=3 or WD ==7) and (datetime_pst.hour >=6 and datetime_pst.hour <18):
            s="A"
            hour_of_shift=datetime_pst.hour-6
        elif (WD > 3 and WD <7) and (datetime_pst.hour >= 6 and datetime_pst.hour < 18):
            s="B"
            hour_of_shift=datetime_pst.hour-6
        elif (WD > 3 and WD <7) and (datetime_pst.hour >= 18 or datetime_pst.hour < 6):
            s="D"
            if datetime_pst.hour < 6:
                hour_of_shift=datetime_pst.hour+6
            elif datetime_pst.hour >= 18:
                hour_of_shift=datetime_pst.hour-18
            if WD==4 and datetime_pst.hour < 6:
                s="C"
        elif (WD <=3 or WD ==7) and (datetime_pst.hour >= 18 or datetime_pst.hour < 6):
            s="C"
            if datetime_pst.hour < 6:
                hour_of_shift=datetime_pst.hour+6
            elif datetime_pst.hour >= 18:
                hour_of_shift=datetime_pst.hour-18
    else:
        if (WD <3 or WD ==7) and (datetime_pst.hour >=6 and datetime_pst.hour <18):
            s="A"
            hour_of_shift=datetime_pst.hour-6
        elif (WD >= 3 and WD <7) and (datetime_pst.hour >= 6 and datetime_pst.hour < 18):
            s="B"
            hour_of_shift=datetime_pst.hour-6
        elif (WD ==3) and (datetime_pst.hour < 6):
            s="C"
            if datetime_pst.hour < 6:
                hour_of_shift=datetime_pst.hour+6
            elif datetime_pst.hour >= 18:
                hour_of_shift=datetime_pst.hour-18
        elif (WD ==3) and (datetime_pst.hour >= 18):
            s="D"
            if datetime_pst.hour < 6:
                hour_of_shift=datetime_pst.hour+6
            elif datetime_pst.hour >= 18:
                hour_of_shift=datetime_pst.hour-18
        elif (WD > 3 and WD <7) and (datetime_pst.hour >= 18 or datetime_pst.hour < 6):
            s="D"
            if datetime_pst.hour < 6:
                hour_of_shift=datetime_pst.hour+6
            elif datetime_pst.hour >= 18:
                hour_of_shift=datetime_pst.hour-18
        elif (WD <3 or WD ==7) and (datetime_pst.hour >= 18 or datetime_pst.hour < 6):
            s="C"
            if datetime_pst.hour < 6:
                hour_of_shift=datetime_pst.hour+6
            elif datetime_pst.hour >= 18:
                hour_of_shift=datetime_pst.hour-18
    hour_of_shift=hour_of_shift+1
    logger('shift',datetime.utcnow(),env,'Ended')
    return s,datetime_pst.hour,date,hour_of_shift
