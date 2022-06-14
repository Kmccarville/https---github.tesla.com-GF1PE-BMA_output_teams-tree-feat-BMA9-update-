# from contextlib import nullcontext
import time
import schedule
# from datetime import datetime
# import error_handler
# from test import debug as masterdebug
import logging 
# import urllib3
# from db import db_connector
import requests
from requests.exceptions import Timeout
# import json
# import pandas as pd
# import stash_reader
# from datetime import timedelta
# import helper_creds
import os

from resources.output123 import output123
from resources.output45 import output45
from resources.outputz3 import outputz3
from resources.outputz4 import outputz4

logging.basicConfig(level=logging.INFO)
logging.info("main_active")
debug = False

def run_schedule():
    while 1:
        schedule.run_pending()
        time.sleep(1) 

if __name__ == '__main__':
    if debug==True:
        logging.info("serve_active")
        output123()
  
    elif debug==False:
        env=os.getenv('ENVVAR3')

        logging.info("Code is running...better go catch it!")
        logging.info("Environment: %s", env)

        schedule.every().hour.at(":00").do(output123,env)
        schedule.every().hour.at(":01").do(output45,env)
        # schedule.every().hour.at(":02").do(outputz3)
        # schedule.every().hour.at(":03").do(outputz4)

        if env == "dev":
            logging.info("Run all command executed")
            schedule.run_all(delay_seconds=10)
            logging.info("Run all command complete")

        logging.info("Hourly run schedule initiated")
        run_schedule()
