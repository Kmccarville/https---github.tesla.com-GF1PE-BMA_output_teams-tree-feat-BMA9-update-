import time
import schedule
import logging 
import os
from pytz import timezone

from common import helper_functions

from resources import devHeading
from resources import outputz1
from resources import outputz2_123
from resources import outputz2_45
from resources import outputz2_8
from resources import outputz2_8_Rev2
from resources import outputz3
from resources import outputz4
from resources import close_nc_check
from resources import eos
from resources import AGV_Spur_Picks
from resources.alerts import cta45_ct
from resources.alerts import cta123_fixtures
from resources.alerts import z2_fixtures
from resources.alerts import bma123_hipot
from resources.alerts import bma123_c3a_dispense

from resources.passdown import cta123_eqt_email

logging.basicConfig(level=logging.INFO)
logging.info("main_active")

if __name__ == '__main__':

    branchName=os.getenv('ENVVAR1')
    commit=os.getenv('ENVVAR2')
    env=os.getenv('ENVVAR3')
    logging.info("Code is running...better go catch it!")
    logging.info("Environment: %s", env)


    scheduler_hourly = schedule.Scheduler()
    scheduler_alerts = schedule.Scheduler()
    scheduler_passdown = schedule.Scheduler()

    #define hourly scheduler
    scheduler_hourly.every().hour.at(":00").do(outputz1.main,env)
    scheduler_hourly.every().hour.at(":00").do(outputz2_123.main,env)
    scheduler_hourly.every().hour.at(":00").do(outputz2_45.main,env)
    scheduler_hourly.every().hour.at(":00").do(outputz2_8.main,env)
    scheduler_hourly.every().hour.at(":00").do(outputz3.main,env)
    scheduler_hourly.every().hour.at(":00").do(outputz4.main,env)
#     scheduler_hourly.every().hour.at(":00").do(outputz2_8_Rev2.main,env)
    scheduler_hourly.every().hour.at(":00").do(close_nc_check.main,env)
    scheduler_hourly.every().hour.at(":02").do(eos.main,env)
    scheduler_hourly.every().hour.at(":00").do(AGV_Spur_Picks.main,env)

    #define alert scheduler
    scheduler_alerts.every().hour.at(":00").do(cta123_fixtures.main,env)
    scheduler_alerts.every().hour.at(":00").do(z2_fixtures.main,env)
    scheduler_alerts.every().hour.at(":00").do(bma123_hipot.main,env)
    scheduler_alerts.every().hour.at(":00").do(bma123_c3a_dispense.main,env)

    #define passdown scheduler
    scheduler_passdown.every().day.at("14:30").do(cta123_eqt_email.main,env)
    scheduler_passdown.every().day.at("02:30").do(cta123_eqt_email.main,env)
    if env == "dev":
        logging.info("BranchName: %s", branchName)
        logging.info("CommitHash: %s", commit)
        logging.info("Send Dev Heading")
        devHeading.main()
        logging.info("Run all command executed")
        scheduler_hourly.run_all(delay_seconds=10)
        scheduler_alerts.run_all(delay_seconds=10)
        scheduler_passdown.run_all(delay_seconds=10)
        devHeading.main(start=False)
        logging.info("Run all command complete. Quiting Program")
        quit()
    else:
        logging.info("Hourly run schedule initiated")
        while True:
            scheduler_hourly.run_pending()
            scheduler_alerts.run_pending()
            scheduler_passdown.run_pending()
            time.sleep(1)
