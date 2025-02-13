import base64
import binascii
import io
import json
import logging
import math
import os
import re
import sys
import time as t
import urllib
from datetime import datetime, timedelta

import common.error_handler as error_handler
import common.helper_creds as helper_creds
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymysql
import pytz
import six
from matplotlib import rcParams
from mysql import connector
from pytz import timezone
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)


def db_connector(write,db,**kwargs): #db=MOS,PLC,Pallet,prodengdb kwargs=**sql,**schema,**dataframe,**append,**table_name,**includeIndex,**returnInsertId
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
                engine=create_engine(f"mysql+pymysql://{helper_creds.get_mos_db()['user']}:{helper_creds.get_mos_db()['password']}@{helper_creds.get_mos_db()['host']}:3307/{helper_creds.get_mos_db()['schema']}")
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
                    engine=create_engine(f"mysql+pymysql://{helper_creds.get_mos_rpt2_db()['user']}:{helper_creds.get_mos_rpt2_db()['password']}@{helper_creds.get_mos_rpt2_db()['host']}:3307/{helper_creds.get_mos_rpt2_db()['schema']}")
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
                engine=create_engine(f"mysql+pymysql://{helper_creds.get_mos_db()['user']}:{helper_creds.get_mos_db()['password']}@{helper_creds.get_mos_db()['host']}:3307/{helper_creds.get_mos_db()['schema']}")
                con_db=engine.connect()
                df=pd.read_sql(sql,con=con_db)
                con_db.close()
            except Exception as e:
                print (eval)
                error_handler.e_handler(e)
                con_db.close()
        elif replication_error==True and rpt2==True:
            try:
                engine=create_engine(f"mysql+pymysql://{helper_creds.get_mos_rpt2_db()['user']}:{helper_creds.get_mos_rpt2_db()['password']}@{helper_creds.get_mos_rpt2_db()['host']}:3307/{helper_creds.get_mos_rpt2_db()['schema']}")
                con_db=engine.connect()
                df=pd.read_sql(sql,con=con_db)
                con_db.close()
            except Exception as e:
                print (e)
                error_handler.e_handler(e)
                con_db.close()

    if db=="PLC" and "sql" in kwargs and write==False:
        plc_creds = helper_creds.get_plc_db()
        sql=kwargs['sql']
        try:
            engine=create_engine(f"mysql+pymysql://{plc_creds['user']}:{plc_creds['password']}@{plc_creds['host']}:{plc_creds['port']}/{plc_creds['db']['info_eq_module']}")
            con_db=engine.connect()
            df=pd.read_sql(sql,con=con_db)
            con_db.close()
        except Exception as e:
            print (e)
            error_handler.e_handler(e)
            con_db.close()

    if db=="ICT" and "sql" in kwargs and write==False:
        ict_creds = helper_creds.get_ict_db()
        sql=kwargs['sql']
        try:
            engine=create_engine(f"mysql+pymysql://{ict_creds['user']}:{ict_creds['password']}@{ict_creds['host']}:{ict_creds['port']}/{ict_creds['db']}")
            con_db=engine.connect()
            df=pd.read_sql(sql,con=con_db)
            con_db.close()
        except Exception as e:
            print (e)
            error_handler.e_handler(e)
            con_db.close()           
            
    return df
