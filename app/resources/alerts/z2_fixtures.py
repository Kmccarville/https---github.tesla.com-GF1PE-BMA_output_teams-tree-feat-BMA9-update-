import common.helper_functions as helper_functions

from datetime import datetime
from datetime import timedelta
from sqlalchemy import text

import logging
import pandas as pd
import numpy as np

def get_fixture_table():
    mos_con = helper_functions.get_sql_conn('mos_rpt2',schema='sparq')
    query = f"""
            select Line, Type,count(distinct(Carrier_ID)) as 'Count'
            from(
                    select  distinct td.valuetext as 'Carrier_ID',
                    CASE WHEN actor.name IN ('3BM1-40000-01') THEN 'Line 1'
                    WHEN actor.name IN ('3BM2-40000-01')THEN 'Line 2'
                    WHEN actor.name IN ('3BM3-40000-01')THEN 'Line 3' 
                    WHEN actor.name in ('MMAMC2.0-LINE3-B','3BM8-42000-01', '3BM8-44000-01') THEN 'Line 8'
                    end as 'Line',

                    CASE WHEN parameter.name IN ('MMAMC Table ID') THEN 'NMAMC Pallet ID' 
                    WHEN task.name in ('NIC_CARRIER') then 'NIC Clamshell Carrier'
                    WHEN task.name in ('IC_CARRIER') then 'IC Clamshell Carrier'
                    else parameter.name
                    end as "Type"
                    from thingdata td
                    inner join thingpath on thingpath.thingid = td.thingid
                    inner join parameter on parameter.id = td.parameterid
                    inner join task on task.id = parameter.taskid
                    inner join actor on actor.id = thingpath.actormodifiedby
                    where td.parameterid in (select id from parameter where name in ('IC Clamshell Carrier','Picture Frame RFID','NIC Clamshell Carrier','MMAMC Table ID','Carrier_ID'))
                    and actor.name in ('3BM1-40000-01', '3BM2-40000-01', '3BM3-40000-01', 'MMAMC2.0-LINE3-B','3BM8-42000-01', '3BM8-44000-01')
                    and td.created > now()- interval 3 hour

                    UNION

                    select
                    distinct td.valuetext as 'Carrier_ID',
                    CASE WHEN actor.name IN ('3BM1-29500-01','3BM1-29400-01') THEN 'Line 1'
                    WHEN actor.name IN ('3BM2-29500-01','3BM2-29400-01')THEN 'Line 2'
                    WHEN actor.name IN ('3BM3-29500-01','3BM3-29400-01')THEN 'Line 3' end as 'Line',
                    parameter.name as "Type"
                    from thingdata td
                    inner join thingpath on thingpath.thingid = td.thingid
                    inner join parameter on parameter.id = td.parameterid
                    inner join actor on actor.id = thingpath.modifiedby
                    where td.parameterid in (select id from parameter where name in ('NMAMC Pallet ID'))
                    and actor.name in ('3BM1-29400-01', '3BM2-29400-01', '3BM3-29400-01')
                    and td.created > now()- interval 3 hour
                    ) sub
            GROUP BY 1,2
            ORDER BY 1,2
            """
    # get df
    df = pd.read_sql(text(query), mos_con)
    df = df.astype(str)
    # replace mos parameter to common abbreviation
    df = df.replace('IC Clamshell Carrier','IC Carrier')
    df = df.replace('NIC Clamshell Carrier','NIC Carrier')
    df = df.replace('NMAMC Pallet ID','Nested Pallet')
    df = df.replace('Picture Frame RFID','Picture Frame')
    # pivot df for each line and reindex
    df = df.pivot(index='Type',columns='Line',values='Count')
    df = df.replace(np.nan,'---')
    df = df.reset_index()
    df = df.rename_axis(None,axis=1)
    # custom sort
    df['Type'] = pd.Categorical(df['Type'],['IC Carrier','NIC Carrier','Picture Frame','Nested Pallet'])
    df = df.sort_values('Type')

    print(df)
    mos_con.close()
    return df

def main(env):
    lookback=3
    now=datetime.utcnow()
    pst_now = helper_functions.convert_from_utc_to_pst(now)
    #only run this script at hours 1,4,7,10,13,16,19,22

    # if pst_now.hour%3 == 1 or env=='dev':

    logging.info("CTA123 Puck-Fixture Alert %s" % datetime.utcnow())

    df = get_fixture_table()
    webhook_key = 'teams_webhook_Zone1_Alerts' if env=='prod' else 'teams_webhook_DEV_Updates'
    title = 'BMA123 Z2 Fixture Count | Active Last 3 Hours'
    caption = 'L123 Goals: IC NIC = 27 | PF = 62 | NP = 8'
    helper_functions.send_alert(webhook_key,title,df,caption)
    logging.info("Sent Alert for CTA123")