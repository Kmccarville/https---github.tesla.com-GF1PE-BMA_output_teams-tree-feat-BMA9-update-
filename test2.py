import time
import schedule
from datetime import datetime
import error_handler
from test import debug as masterdebug
import logging 
import urllib3
from db import teams_webhook
from db import db_connector
import requests
import json
import pandas 
from stash_reader import bmaoutput
from tabulate import tabulate
import numpy as np

logging.basicConfig(level=logging.INFO)
logging.info("main_active")
debug=masterdebug

def uph_calculation(df):
    ACTA1  =[]
    ACTA2 =[] 
    ACTA3 =[]
    NESTED1 =[]
    NESTED2 =[]
    NESTED3 = []
    AC3A1 =[]
    AC3A2 =[]
    AC3A3 = []
    if len(df.index)>0:
        for index, row in df.iterrows():  
            if row['ActorModifiedby']  =='3BM1-26400-01':
                ACTA1.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM2-26400-01':
                ACTA2.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM3-26400-01':
                ACTA3.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM1-29500-01':
                NESTED1.append(f"{row['Thingname']}") 
            elif row['ActorModifiedby']  =='3BM2-29500-01':
                NESTED2.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM3-29500-01':
                NESTED3.append(f"{row['Thingname']}") 
            elif row['ActorModifiedby']  =='3BM1-40001-01':
                AC3A1.append(f"{row['Thingname']}")  
            elif row['ActorModifiedby']  =='3BM2-40001-01':
                AC3A2.append(f"{row['Thingname']}")  
            elif row['ActorModifiedby']  =='3BM3-40001-01':
                AC3A3.append(f"{row['Thingname']}") 

    a_list = [['BMA','ACTA UPH','NMAMC UPH','AC3A UPH'],['BMA1', len(ACTA1)/28, len(NESTED1)/4 , len(AC3A1)/4],['BMA2', len(ACTA2)/28, len(NESTED2)/4 , len(AC3A2)/4], ['BMA3', len(ACTA3)/28, len(NESTED3)/4 , len(AC3A3)/4]]
    list_as_array = np.array(a_list)                                           
    return (list_as_array)
    #return tabulate([['BMA1', len(ACTA1)/28, len(NESTED1)/4 , len(AC3A1)/4],['BMA2', len(ACTA2)/28, len(NESTED2)/4 , len(AC3A2)/4], ['BMA3', len(ACTA3)/28, len(NESTED3)/4 , len(AC3A3)/4]], headers=['BMA','ACTA UPH','NMAMC UPH','AC3A UPH'])

def output():
    #grab hourly data
    sql=bmaoutput()
    df=db_connector(False,"MOS",sql=sql) 
    output_string= uph_calculation(df)
    print (output_string)
    title='Hourly Summary'
    message='message'
    payload={"title":title, 
    "summary":"summary",
    "sections":[{"activitySubtitle" : output_string}]}
    #post to BMA123-PE --> Output Channel
    response = requests.post(teams_webhook, json.dumps(payload))

output()

def run_schedule():
    while 1:
        schedule.run_pending()
        time.sleep(1) 

if __name__ == '__main__':
    if debug==True:
        logging.info("serve_active")
    elif debug==False:
        schedule.every().hour.at(":01").do(output)
        logging.info("serve_active")