import logging
from datetime import datetime, timedelta

import common.helper_functions as helper_functions
import numpy as np
import pandas as pd
from sqlalchemy import text


def get_hipot_table():
    mos_con = helper_functions.get_sql_conn('mos_rpt2',schema='sparq')
    query = f"""
        SELECT
            parameter.name as 'PARAMETER',
	        (case left(actor.name,4)
            when '3BM1' then 'BMA 1'
            when '3BM2' then 'BMA 2'
            when '3BM3' then 'BMA 3'
            when '3BM8' then 'BMA 8'
            end) as 'LINE',
            COUNT((CASE WHEN thingdata.parameterresult = 'passed' THEN 1 END)) as 'GOOD',
            COUNT((CASE WHEN thingdata.parameterresult = 'failed' THEN 1 END)) as 'BAD',
            COUNT((CASE WHEN thingdata.parameterresult = 'passed' THEN 1 END))/(COUNT((CASE WHEN thingdata.parameterresult = 'failed' THEN 1 END))+COUNT((CASE WHEN thingdata.parameterresult = 'passed' THEN 1 END))) as YIELD
        FROM sparq.thingdata
            JOIN thing ON thing.id = thingdata.thingid
            JOIN actor ON actor.id = thingdata.actormodifiedby
            JOIN parameter ON parameter.id = thingdata.parameterid

        WHERE
	        thingdata.created >= NOW() - INTERVAL 1 HOUR
		        and thingdata.taskid in (select task.id from task where task.name in ('NMAMC Hipot','SAMAMC Hipot','MAMC Hipot'))
		        and parameter.name not like '%Voltage'
		        and actor.type = 'EQUIPMENT'
        group by 1, 2
        order by 2, 1
            """
    # get df
    df = pd.read_sql(text(query), mos_con)
    df = df.astype({"PARAMETER": str, "LINE": str, "GOOD": int, "BAD": int, "YIELD": int})
    mos_con.close()
    # replace mos parameter to common abbreviation
    df = df.replace('NMAMC ACW Hipot Bandolier 1','ACW1')
    df = df.replace('NMAMC ACW Hipot Bandolier 2','ACW2')
    df = df.replace('NMAMC ACW Hipot Bandolier 3','ACW3')
    df = df.replace('NMAMC ACW Hipot Bandolier 4','ACW4')
    df = df.replace('NMAMC ACW Hipot Bandolier 5','ACW5')
    df = df.replace('NMAMC ACW Hipot Bandolier 6','ACW6')
    df = df.replace('NMAMC ACW Hipot Bandolier 7','ACW7')

    df = df.replace('NMAMC DCW Hipot Bandolier 1','DCW1')
    df = df.replace('NMAMC DCW Hipot Bandolier 2','DCW2')
    df = df.replace('NMAMC DCW Hipot Bandolier 3','DCW3')
    df = df.replace('NMAMC DCW Hipot Bandolier 4','DCW4')
    df = df.replace('NMAMC DCW Hipot Bandolier 5','DCW5')
    df = df.replace('NMAMC DCW Hipot Bandolier 6','DCW6')
    df = df.replace('NMAMC DCW Hipot Bandolier 7','DCW7')


    # pivot df for each LINE and reindex
    df = df.pivot(index='LINE',columns='PARAMETER',values='BAD')
    #df = df.replace(np.nan,'---')
    df = df.reset_index()
    df = df.rename_axis(None,axis=1)
    # custom sort
    df['LINE'] = pd.Categorical(df['LINE'],['BMA 1', 'BMA 2', 'BMA 3', 'BMA 8'])
    df = df.sort_values('LINE')

    return df

def main(env):
    lookback=1
    now=datetime.utcnow()
    pst_now = helper_functions.convert_from_utc_to_pst(now)
    if pst_now.hour%1 == 0 or env=='dev':

        logging.info("BMA123 Hipot Alert %s" % datetime.utcnow())

        df = get_hipot_table()
        webhook_key = 'teams_webhook_BMA123_OCAP_Alerts' if env=='prod' else 'teams_webhook_DEV_Updates'
        title = 'ALERT: BMA123 Z2 Hipot'
        caption = '1 Hour Interval, Perform OCAP Where Fail Count >= 2'
        link_title = "Link to OCAP"
        link_button = "https://confluence.teslamotors.com/pages/viewpage.action?spaceKey=PRODENG&title=Nested+OCAP+Troubleshoot+Guide&preview=/603675295/744753488/Nested%20HiPot%20OCAP%20Troubleshoot%20Guide%20-%20Flowchart.png"
        if (df.loc[:,'ACW1':] >= 2).any().any():
            helper_functions.send_alert(webhook_key,title,df,caption,link_title,link_button)
            logging.info("Sent Alert for BMA123")
        else: logging.info("Alert not sent for BMA123")
