from mysql import connector
from datetime import datetime
from datetime import timedelta
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import os
import time as t
from test import debug as masterdebug
import logging

logging.basicConfig(level=logging.INFO)

debug=masterdebug
if debug == True:
    with open('pass_rpivision.txt') as f:
        lines = f.readlines()
        f.close()
elif debug ==False:
    with open(r'/app/secrets/credentials') as f:
        lines = f.readlines()
        f.close()
#prodeng db connection information
localhost=str(lines[0])
localhost=str.strip(localhost)
user=str(lines[1])
user=str.strip(user)
passwd=str(lines[2])
passwd=str.strip(passwd)
database=str(lines[3])
database=str.strip(database)

def reporter (device,timestamp):
    logging.info("reporter_active")
    dtdf=pd.DataFrame()
    engine=create_engine(f"mysql+pymysql://{user}:{passwd}@{localhost}/{database}")
    con_db=engine.connect()
    d=[device]
    t=[timestamp]
    dtdf.insert(loc=0, column='datetime', value=t)
    dtdf.insert(loc=1, column='device', value=d)
    dtdf.to_sql(name='runtime',con=con_db,if_exists="append")
    con_db.close()
    msg=f"succesfully reported heartbeat for {device}"
    return print(msg)

