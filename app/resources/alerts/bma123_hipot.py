import common.helper_functions as helper_functions

from datetime import datetime
from datetime import timedelta
from sqlalchemy import text

import logging
import pandas as pd
import numpy as np

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
	        thingdata.created between '{start}' and '{end}'
		        and thingdata.taskid in (select task.id from task where task.name in ('NMAMC Hipot','SAMAMC Hipot','MAMC Hipot'))
		        and parameter.name not like '%Voltage'
		        and actor.type = 'EQUIPMENT'
        group by 1, 2
        order by 2, 1
            """
    # get df
    df = pd.read_sql(text(query), mos_con)
    df = df.astype(str)
    mos_con.close()
    # replace mos parameter to common abbreviation
    df = df.replace('NMAMC ACW Hipot Bandolier 1','ACW Bando 1')
    df = df.replace('NMAMC ACW Hipot Bandolier 2','ACW Bando 2')
    df = df.replace('NMAMC ACW Hipot Bandolier 3','ACW Bando 3')
    df = df.replace('NMAMC ACW Hipot Bandolier 4','ACW Bando 4')
    df = df.replace('NMAMC ACW Hipot Bandolier 5','ACW Bando 5')
    df = df.replace('NMAMC ACW Hipot Bandolier 6','ACW Bando 6')
    df = df.replace('NMAMC ACW Hipot Bandolier 7','ACW Bando 7')

    df = df.replace('NMAMC DCW Hipot Bandolier 1','DCW Bando 1')
    df = df.replace('NMAMC DCW Hipot Bandolier 2','DCW Bando 2')
    df = df.replace('NMAMC DCW Hipot Bandolier 3','DCW Bando 3')
    df = df.replace('NMAMC DCW Hipot Bandolier 4','DCW Bando 4')
    df = df.replace('NMAMC DCW Hipot Bandolier 5','DCW Bando 5')
    df = df.replace('NMAMC DCW Hipot Bandolier 6','DCW Bando 6')
    df = df.replace('NMAMC DCW Hipot Bandolier 7','DCW Bando 7')

    # pivot df for each LINE and reindex
    df = df.pivot(index='LINE',columns='PARAMETER',values='BAD')
    df = df.replace(np.nan,'---')
    df = df.reset_index()
    df = df.rename_axis(None,axis=1)
    # custom sort
    df['LINE'] = pd.Categorical(df['LINE'],['NMAMC ACW Hipot Bandolier 1', 'NMAMC ACW Hipot Bandolier 2', 'NMAMC ACW Hipot Bandolier 3', 'NMAMC ACW Hipot Bandolier 4', 'NMAMC ACW Hipot Bandolier 5', 'NMAMC ACW Hipot Bandolier 6', 'NMAMC ACW Hipot Bandolier 7','NMAMC DCW Hipot Bandolier 1', 'NMAMC DCW Hipot Bandolier 2', 'NMAMC DCW Hipot Bandolier 3', 'NMAMC DCW Hipot Bandolier 4', 'NMAMC DCW Hipot Bandolier 5', 'NMAMC DCW Hipot Bandolier 6', 'NMAMC DCW Hipot Bandolier 7',])
    df = df.sort_values('LINE')
    # define goal column
    #goals = [27,27,65,8]
    #df['GOAL'] = goals

    return df

def main(env):
    lookback=3
    now=datetime.utcnow()
    pst_now = helper_functions.convert_from_utc_to_pst(now)
    #only run this script at hours 1,4,7,10,13,16,19,22

    if pst_now.hour%3 == 1 or env=='dev':

        logging.info("BMA123 Hipot Alert %s" % datetime.utcnow())

        df = get_hipot_table()
        webhook_key = 'teams_webhook_BMA123_OCAP_Alerts' if env=='prod' else 'teams_webhook_DEV_Updates'
        title = 'BMA123 Z2 Hipot Alert'
        caption = 'Count Last 3 Hours'
        helper_functions.send_alert(webhook_key,title,df,caption)
        logging.info("Sent Alert for BMA123")