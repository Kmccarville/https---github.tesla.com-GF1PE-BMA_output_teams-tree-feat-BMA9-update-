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

logging.basicConfig(level=logging.INFO)
logging.info("main_active")
debug=masterdebug

def output():
    #grab hourly data
    sql=bmaoutput()
    print (sql)
    df=db_connector(False,"MOS",sql=sql) 
    print (df)

    title='Hourly Summary'
    message='message'
    payload={"title":title, 
    "summary":"summary",
    "sections":[{"activitySubtitle" : message}]
    }
    #post to BMA123-PE --> Output Channel
    #response = requests.post(teams_webhook, json.dumps(payload))

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
        



