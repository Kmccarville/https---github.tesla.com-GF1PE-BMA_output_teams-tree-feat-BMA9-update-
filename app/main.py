import logging
import os
import time

import schedule
from common import helper_functions
from pytz import timezone
from resources import (AGV_Spur_Picks, NCM_bandolier_milan_output,
                       NCM_module_output, close_nc_check, devHeading, eos,
                       outputz1, outputz2_8, outputz2_8_Rev2, outputz2_45,
                       outputz2_123, outputz3, outputz4, staffing)
from resources.alerts import (bma123_c3a_dispense, bma123_hipot,
                              bma123_Z2_FOD_weekly, cta45_ct, cta123_fixtures,
                              z2_contamination, z2_fixtures)
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
    scheduler_hourly.every().hour.at(":00").do(close_nc_check.main,env)
    scheduler_hourly.every().hour.at(":02").do(eos.main,env)
    scheduler_hourly.every().hour.at(":00").do(AGV_Spur_Picks.main,env)
    scheduler_alerts.every().hour.at(":00").do(NCM_bandolier_milan_output.main,env)

    #define alert scheduler
    scheduler_alerts.every().hour.at(":00").do(z2_contamination.main,env)
    scheduler_alerts.every().hour.at(":00").do(z2_fixtures.main,env)
    scheduler_alerts.every().hour.at(":00").do(bma123_hipot.main,env)
    scheduler_alerts.every().hour.at(":00").do(bma123_c3a_dispense.main,env)


    #define staffing scheduler
    scheduler_passdown.every().day.at("14:35").do(staffing.main,env)
    scheduler_passdown.every().day.at("02:35").do(staffing.main,env)
    
    if env == "dev":
        logging.info("BranchName: %s", branchName)
        logging.info("CommitHash: %s", commit)
        logging.info("Send Dev Heading")
        devHeading.main()
        logging.info("Run all command executed")
        scheduler_hourly.run_all(delay_seconds=10)
        scheduler_alerts.run_all(delay_seconds=10)
        scheduler_passdown.run_all(delay_seconds=10)
        eos.main(env,local_run=True)
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

