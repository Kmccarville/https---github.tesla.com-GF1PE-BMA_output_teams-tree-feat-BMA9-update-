import common.helper_functions as helper_functions

from datetime import datetime
from datetime import timedelta
from sqlalchemy import text

import logging
import pandas as pd
import numpy as np

def get_puck_table():
    query = f"""
        SELECT 
            LEFT(equipment, 4) AS LINE,
            COUNT(DISTINCT puck_id) AS 'PUCKS'
        FROM
            PUCK_HIST
        WHERE
            EQUIPMENT LIKE '3BM_-10%'
                AND logged > NOW() - INTERVAL 12 HOUR
        GROUP BY 1
        ORDER BY LINE ASC
        """
    celldb_con = helper_functions.get_sql_conn('cell_db_rpt',schema='gf1_cell')
    df = pd.read_sql(text(query), celldb_con)
    celldb_con.close()
    return df

def get_fixture_table():
    tables = [44,46,41]
    count = []
    plc_con = helper_functions.get_sql_conn('plc_db',schema='rno_ia_taghistory_batterymodule')
    df = pd.DataFrame()
    for table in tables:
        query = f"""
            select sqlth.intvalue
            from sqlth_{table}_data sqlth
            left join sqlth_te te on sqlth.tagid = te.id
            left join sqlth_scinfo sc on te.scid = sc.id
            left join sqlth_drv drv on sc.drvid = drv.id
            where
                te.tagpath = 'Global/FixtureManagement/FixtureNumber' and
                drv.provider LIKE 'CTA%'
            ORDER BY t_stamp DESC
            LIMIT 1
        """
        df = pd.read_sql(text(query), plc_con)
        count.append(df.get_value(0,'intvalue'))
    plc_con.close()
    d = {'LINE': ['3BM1','3BM2','3BM3'],'FIXTURES': count}
    df = pd.DataFrame(data=d)
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

        df_puck = get_puck_table()
        df_fixt = get_fixture_table()
        df_rej_fixt = get_reject_fixture_table()
        goal= {'LINE': 'GOAL','PUCKS': 5200,'FIXTURES': 185,'-90s': '--'}
        
        df = pd.concat([df_puck,df_fixt['FIXTURES'],df_rej_fixt['-90s']],axis=1)
        df = df.append(goal,ignore_index=True)
        
        webhook_key = 'teams_webhook_Zone2_123_Alerts' if env=='prod' else 'teams_webhook_DEV_Updates'
        title = 'CTA123 Puck and Fixture Count'
        caption = ''
        helper_functions.send_alert(webhook_key,title,df,caption)
        logging.info("Sent Alert for CTA123")