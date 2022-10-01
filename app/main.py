import time
import schedule
import logging 
import os

from common import helper_functions

from resources.output123 import output123
from resources.output45 import output45
from resources.outputz3 import outputz3
from resources.outputz4 import outputz4
from resources.eos import eos_report

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
        schedule.every().hour.at(":00").do(output45,env)
        schedule.every().hour.at(":00").do(outputz3,env)
        schedule.every().hour.at(":00").do(outputz4,env)
        schedule.every().hour.at(":00").do(eos_report,env)

        if env == "dev":
            logging.info("Run all command executed")
            schedule.run_all(delay_seconds=10)
            logging.info("Run all command complete")

        logging.info("Hourly run schedule initiated")
        run_schedule()
