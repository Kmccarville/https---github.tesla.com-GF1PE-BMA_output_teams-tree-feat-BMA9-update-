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
from resources import NCM_bandolier_milan_output
from resources import NCM_module_output
from resources import staffing
from resources.alerts import cta45_ct
from resources.alerts import cta123_fixtures
from resources.alerts import z2_fixtures
from resources.alerts import bma123_hipot
from resources.alerts import bma123_c3a_dispense
from resources.alerts import z2_contamination
from resources.alerts import bma123_Z2_FOD_weekly

from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from resources.passdown import cta123_eqt_email

logging.basicConfig(level=logging.INFO)
logging.info("main_active")


if __name__ == '__main__':
    PST_TZ = "America/Los_Angeles"
        
    branchName=os.getenv('ENVVAR1')
    commit=os.getenv('ENVVAR2')
    env=os.getenv('ENVVAR3')
    logging.info("Code is running...better go catch it!")
    logging.info("Environment: %s", env)

    if env == "dev":
        logging.info("BranchName: %s", branchName)
        logging.info("CommitHash: %s", commit)
        logging.info("Send Dev Heading")
        devHeading.main()
        
    scheduler = BlockingScheduler()
    
    #define hourly scheduler
    scheduler.add_job(outputz1.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env])
    scheduler.add_job(outputz2_123.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env])
    scheduler.add_job(outputz2_45.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env])
    scheduler.add_job(outputz2_8.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env])
    scheduler.add_job(outputz3.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env])
    scheduler.add_job(outputz4.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env])
    scheduler.add_job(close_nc_check.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env])
    scheduler.add_job(eos.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env])
    scheduler.add_job(AGV_Spur_Picks.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env])
    scheduler.add_job(NCM_bandolier_milan_output.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env])

    #define alert scheduler
    scheduler.add_job(z2_contamination.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env])
    scheduler.add_job(z2_fixtures.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env])
    scheduler.add_job(bma123_hipot.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env])
    scheduler.add_job(bma123_c3a_dispense.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env])

    #define staffing scheduler
    scheduler.add_job(staffing.main, CronTrigger.from_crontab('35 6,18 * * *', PST_TZ), args=[env])
        
    scheduler.start()
