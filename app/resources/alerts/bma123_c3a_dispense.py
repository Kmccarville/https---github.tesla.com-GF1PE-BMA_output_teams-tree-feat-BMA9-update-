import common.helper_functions as helper_functions

from datetime import datetime
from datetime import timedelta
from sqlalchemy import text

import logging
import pandas as pd
import numpy as np

def get_dispense_table():
    mos_con = helper_functions.get_sql_conn('mos_rpt2',schema='sparq')
    query = f"""
        SELECT
            left(actor.name,4) as LINE,
            parameter.name as 'PARAMETER',
    	    Count((CASE WHEN thingdata.valuetext = 0 THEN 1 END)) as GOOD,
    	    sum(thingdata.valuetext) as BAD,
    	    (Count((CASE WHEN thingdata.valuetext = 0 THEN 1 END))/(sum(thingdata.valuetext) + Count((CASE WHEN thingdata.valuetext = 0 THEN 1 END)))) AS YIELD
        FROM sparq.thingdata
            JOIN sparq.thing ON thing.id = thingdata.thingid
            INNER JOIN sparq.actor ON actor.id = thingdata.actormodifiedby
            INNER JOIN sparq.parameter ON parameter.id = thingdata.parameterid
        WHERE
    	    thingdata.created >= now() - interval 1 hour
    	    and thingdata.taskid in (select task.id from sparq.task where task.name in ('ClamshellClose001'))
            and thingdata.parameterid in (select parameter.id from sparq.parameter where parameter.name in ('IC Fail Count','NIC Fail Count'))
    	    and actor.type = 'EQUIPMENT'
        group by parameter.name, actor.name
        order by LINE
            """
    # get df
    df = pd.read_sql(text(query), mos_con)
    df = df.astype({"PARAMETER": str, "LINE": str, "GOOD": int, "BAD": int, "YIELD": float})
    mos_con.close()

    #rename line to common abbreviation
    df = df.replace('3BM1','BMA 1')
    df = df.replace('3BM2','BMA 2')
    df = df.replace('3BM3','BMA 3')

    df['YIELD'] = df['YIELD']*100
    df = df.round({'YIELD': 2})
    df = df.astype({"YIELD": str})
    df.loc[df['YIELD'] == '100.0', 'YIELD'] = '100'
    df['YIELD'] = df['YIELD'] + ' %'

    df = df.sort_values(['LINE', 'PARAMETER'])

    return df

def main(env):
    lookback=1
    now=datetime.utcnow()
    pst_now = helper_functions.convert_from_utc_to_pst(now)

    logging.info("BMA123 C3A Dispense Alert %s" % datetime.utcnow())

    df = get_dispense_table()
    webhook_key = 'teams_webhook_BMA123_OCAP_Alerts' if env=='prod' else 'teams_webhook_DEV_Updates'
    title = 'ALERT: BMA123 C3A Dispense'
    caption = '1 Hour Interval, Perform OCAP Where Fail Count >= 3'
    link_title = "Link to OCAP"
    link_button = "https://confluence.teslamotors.com/display/PRODENG/Dispense+-+Out+of+Control+Action+Plan"
    if (df.loc[:,'BAD':'BAD'] >= 3).any().any():
        helper_functions.send_alert(webhook_key,title,df,caption,link_title,link_button)
        logging.info("Sent Alert for BMA123")
    else: logging.info("Alert not sent for BMA123")
