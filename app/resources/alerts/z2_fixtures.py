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
            select  distinct td.valuetext as 'Carrier ID',
            CASE WHEN actor.name IN ('3BM1-40000-01') THEN 'Line 1'
            WHEN actor.name IN ('3BM2-40000-01')THEN 'Line 2'
            WHEN actor.name IN ('3BM3-40000-01')THEN 'Line 3' 
            WHEN actor.name in ('MMAMC2.0-LINE3-B','3BM8-42000-01', '3BM8-44000-01') THEN 'Line 8'
            end as 'Line',

            CASE WHEN parameter.name IN ('MMAMC Table ID') THEN 'NMAMC Pallet ID' 
            WHEN task.name in ('NIC_CARRIER') then 'NIC Clamshell Carrier'
            WHEN task.name in ('IC_CARRIER') then 'IC Clamshell Carrier'
            else parameter.name
            end as "Carrier_type"
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
            distinct td.valuetext as 'Carrier ID',
            CASE WHEN actor.name IN ('3BM1-29500-01','3BM1-29400-01') THEN 'Line 1'
            WHEN actor.name IN ('3BM2-29500-01','3BM2-29400-01')THEN 'Line 2'
            WHEN actor.name IN ('3BM3-29500-01','3BM3-29400-01')THEN 'Line 3' end as 'Line',
            parameter.name as "Carrier_type"
            from thingdata td
            inner join thingpath on thingpath.thingid = td.thingid
            inner join parameter on parameter.id = td.parameterid
            inner join actor on actor.id = thingpath.modifiedby
            where td.parameterid in (select id from parameter where name in ('NMAMC Pallet ID'))
            and actor.name in ('3BM1-29400-01', '3BM2-29400-01', '3BM3-29400-01')
            and td.created > now()- interval 3 hour        
            """
    df = pd.read_sql(text(query), mos_con)
    mos_con.close()

    lines = ['Line 1', 'Line 2', 'Line 3' , 'Line 8']
    pallets = ['IC Clamshell Carrier' , 'NIC Clamshell Carrier' , 'Picture Frame RFID', 'NMAMC Pallet ID', 'MAMC Table']

    for line in lines:
        for pallet in pallets:
            sub_df = df.query(f"Line=='{line}' and Carrier_type=='{pallet}'")
            print(sub_df)
    # d = {'LINE': ['3BM1','3BM2','3BM3','3BM8'],'FIXTURES': count}
    # df = pd.DataFrame(data=d)
    return df

def get_reject_fixture_table():
    query = f"""
        SELECT 
            LEFT(line_id, 4) AS 'LINE',
            COUNT(*) AS '-90s'
        FROM
            gf1_asrs_management.asrs_record
        WHERE
            LINE_ID LIKE 'CTA_%'
            AND PALLET_TYPE <= - 90
            AND POSITION_STATUS = 'FULL'
        GROUP BY 1 ASC
        """
    asrs_con = helper_functions.get_sql_conn('gf1_pallet_management',schema='gf1_asrs_management')
    df = pd.read_sql(text(query), asrs_con)
    asrs_con.close()
    return df

def main(env):
    lookback=3
    now=datetime.utcnow()
    pst_now = helper_functions.convert_from_utc_to_pst(now)
    #only run this script at hours 1,4,7,10,13,16,19,22

    if pst_now.hour%3 == 1 or env=='dev':
        logging.info("CTA123 Puck-Fixture Alert %s" % datetime.utcnow())
        now_sub3=now+timedelta(hours=-lookback)

        # df_puck = get_puck_table()
        df_fixt = get_fixture_table()
        df_rej_fixt = get_reject_fixture_table()
        goal= {'LINE': 'GOAL','PUCKS': 5200,'FIXTURES': 185,'-90s': '--'}
        
        # df = pd.concat([df_puck,df_fixt['FIXTURES'],df_rej_fixt['-90s']],axis=1)
        # df = df.append(goal,ignore_index=True)
        
        webhook_key = 'teams_webhook_Zone1_Alerts' if env=='prod' else 'teams_webhook_DEV_Updates'
        title = 'CTA123 Puck and Fixture Count'
        caption = ''
        # helper_functions.send_alert(webhook_key,title,df,caption)
        logging.info("Sent Alert for CTA123")