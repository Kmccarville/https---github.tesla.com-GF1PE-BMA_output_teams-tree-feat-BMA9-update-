import logging
import os

from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from resources import (AGV_Spur_Picks, NCM_bandolier_milan_output, close_nc_check, devHeading, eos,
                       outputz1, outputz2_8, outputz2_45,
                       outputz2_123, outputz3, outputz4, staffing)
from resources.alerts import (bma123_c3a_dispense, bma123_hipot,
                              z2_contamination, z2_fixtures)

logging.basicConfig(level=logging.INFO)
logging.info("main_active")

if __name__ == '__main__':
    PST_TZ = "America/Los_Angeles"
        
    branchName=os.getenv('ENVVAR1')
    commit=os.getenv('ENVVAR2')
    env=os.getenv('ENVVAR3')
    logging.info("Code is running...better go catch it!")
    logging.info("Environment: %s", env)
    
    scheduler = BlockingScheduler()
    job_list = []
    #define hourly scheduler
    job_list.append(scheduler.add_job(outputz1.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env]))
    job_list.append(scheduler.add_job(outputz2_123.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env]))
    job_list.append(scheduler.add_job(outputz2_45.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env]))
    job_list.append(scheduler.add_job(outputz2_8.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env]))
    job_list.append(scheduler.add_job(outputz3.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env]))
    job_list.append(scheduler.add_job(outputz4.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env]))
    job_list.append(scheduler.add_job(close_nc_check.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env]))
    job_list.append(scheduler.add_job(eos.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env]))
    job_list.append(scheduler.add_job(AGV_Spur_Picks.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env]))
    job_list.append(scheduler.add_job(NCM_bandolier_milan_output.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env]))

    #define alert scheduler
    job_list.append(scheduler.add_job(z2_contamination.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env]))
    job_list.append(scheduler.add_job(z2_fixtures.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env]))
    job_list.append(scheduler.add_job(bma123_hipot.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env]))
    job_list.append(scheduler.add_job(bma123_c3a_dispense.main, CronTrigger.from_crontab('0 * * * *', PST_TZ), args=[env]))

    #define staffing scheduler
    job_list.append(scheduler.add_job(staffing.main, CronTrigger.from_crontab('35 6,18 * * *', PST_TZ), args=[env]))
    
    
    if env == "dev":
        logging.info("BranchName: %s", branchName)
        logging.info("CommitHash: %s", commit)
        logging.info("Send Dev Heading")
        devHeading.main()

        for job in job_list:
            job.modify(next_run_time=datetime.now())
        scheduler.start()

    else:
        scheduler.start()
