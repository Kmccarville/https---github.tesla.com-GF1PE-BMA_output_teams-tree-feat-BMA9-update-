import time
import schedule
import logging 
import os

from common import helper_functions

from resources import output123
from resources import yield123
from resources import output45
from resources import output8
from resources import outputz3
from resources import outputz4
from resources import close_nc_check
from resources import eos

from resources.alerts import cta45_ct

logging.basicConfig(level=logging.INFO)
logging.info("main_active")

if __name__ == '__main__':

    env=os.getenv('ENVVAR3')
    logging.info("Code is running...better go catch it!")
    logging.info("Environment: %s", env)

    scheduler_hourly = schedule.Scheduler()
    scheduler_alerts = schedule.Scheduler()

    #define hourly scheduler
    scheduler_hourly.every().hour.at(":00").do(output123.main,env)
    scheduler_hourly.every().hour.at(":00").do(output45.main,env)
    scheduler_hourly.every().hour.at(":00").do(output8.main,env)
    scheduler_hourly.every().hour.at(":00").do(outputz3.main,env)
    #scheduler_hourly.every().hour.at(":00").do(outputz4.main,env)
    scheduler_hourly.every().hour.at(":00").do(yield123.main,env)
    scheduler_hourly.every().hour.at(":00").do(close_nc_check.main,env)
    scheduler_hourly.every().hour.at(":00").do(eos.main,env)

    #define alert scheduler
    scheduler_alerts.every().hour.at(":05").do(cta45_ct.main,env)

    if env == "dev":
        logging.info("Run all command executed")
        scheduler_hourly.run_all(delay_seconds=10)
        scheduler_alerts.run_all(delay_seconds=10)
        logging.info("Run all command complete")

    logging.info("Hourly run schedule initiated")
    while 1:
        scheduler_hourly.run_pending()
        scheduler_alerts.run_pending()
