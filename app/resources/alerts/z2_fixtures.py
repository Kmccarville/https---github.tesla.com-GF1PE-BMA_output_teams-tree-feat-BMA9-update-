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
            SELECT 
                LINE, TYPE, COUNT(DISTINCT (Carrier_ID)) AS 'Count'
            FROM
                (SELECT DISTINCT
                    td.valuetext AS 'Carrier_ID',
                        CASE
                            WHEN actor.name IN ('3BM1-40000-01') THEN 'LINE 1'
                            WHEN actor.name IN ('3BM2-40000-01') THEN 'LINE 2'
                            WHEN actor.name IN ('3BM3-40000-01') THEN 'LINE 3'
                            WHEN actor.name IN ('MMAMC2.0-LINE3-B' , '3BM8-42000-01', '3BM8-44000-01') THEN 'LINE 8'
                        END AS 'LINE',
                        CASE
                            WHEN parameter.name IN ('MMAMC Table ID') THEN 'NMAMC Pallet ID'
                            WHEN task.name IN ('NIC_CARRIER') THEN 'NIC Clamshell Carrier'
                            WHEN task.name IN ('IC_CARRIER') THEN 'IC Clamshell Carrier'
                            ELSE parameter.name
                        END AS 'TYPE'
                FROM
                    thingdata td
                INNER JOIN thingpath ON thingpath.thingid = td.thingid
                INNER JOIN parameter ON parameter.id = td.parameterid
                INNER JOIN task ON task.id = parameter.taskid
                INNER JOIN actor ON actor.id = thingpath.actormodifiedby
                WHERE
                    td.parameterid IN (SELECT 
                            id
                        FROM
                            parameter
                        WHERE
                            name IN ('IC Clamshell Carrier' , 'Picture Frame RFID', 'NIC Clamshell Carrier', 'MMAMC Table ID', 'Carrier_ID'))
                        AND actor.name IN ('3BM1-40000-01' , '3BM2-40000-01', '3BM3-40000-01', 'MMAMC2.0-LINE3-B')
                        AND td.created > NOW() - INTERVAL 3 HOUR UNION SELECT DISTINCT
                    td.valuetext AS 'Carrier_ID',
                        CASE
                            WHEN actor.name IN ('3BM1-29500-01' , '3BM1-29400-01') THEN 'LINE 1'
                            WHEN actor.name IN ('3BM2-29500-01' , '3BM2-29400-01') THEN 'LINE 2'
                            WHEN actor.name IN ('3BM3-29500-01' , '3BM3-29400-01') THEN 'LINE 3'
                        END AS 'LINE',
                        parameter.name AS 'TYPE'
                FROM
                    thingdata td
                INNER JOIN thingpath ON thingpath.thingid = td.thingid
                INNER JOIN parameter ON parameter.id = td.parameterid
                INNER JOIN actor ON actor.id = thingpath.modifiedby
                WHERE
                    td.parameterid IN (SELECT 
                            id
                        FROM
                            parameter
                        WHERE
                            name IN ('NMAMC Pallet ID'))
                        AND actor.name IN ('3BM1-29400-01' , '3BM2-29400-01', '3BM3-29400-01')
                        AND td.created > NOW() - INTERVAL 3 HOUR) sub
            GROUP BY 1 , 2
            ORDER BY 1 , 2
            """
    # get df
    df = pd.read_sql(text(query), mos_con)
    df = df.astype(str)
    mos_con.close()
    # replace mos parameter to common abbreviation
    df = df.replace('IC Clamshell Carrier','IC Carrier')
    df = df.replace('NIC Clamshell Carrier','NIC Carrier')
    df = df.replace('NMAMC Pallet ID','Nested Pallet')
    df = df.replace('Picture Frame RFID','Picture Frame')
    # pivot df for each LINE and reindex
    df = df.pivot(index='TYPE',columns='LINE',values='Count')
    df = df.replace(np.nan,'---')
    df = df.reset_index()
    df = df.rename_axis(None,axis=1)
    # custom sort
    df['TYPE'] = pd.Categorical(df['TYPE'],['IC Carrier','NIC Carrier','Picture Frame','Nested Pallet'])
    df = df.sort_values('TYPE')
    # define goal column
    goals = [27,27,62,8]
    df['GOAL'] = goals

    return df

def main(env):
    lookback=3
    now=datetime.utcnow()
    pst_now = helper_functions.convert_from_utc_to_pst(now)
    #only run this script at hours 1,4,7,10,13,16,19,22

    if pst_now.hour%3 == 1 or env=='dev':

        logging.info("CTA123 Puck-Fixture Alert %s" % datetime.utcnow())

        df = get_fixture_table()
        webhook_key = 'teams_webhook_Zone1_Alerts' if env=='prod' else 'teams_webhook_DEV_Updates'
        title = 'BMA123 Z2 Fixture Count'
        caption = 'Active Last 3 Hours'
        helper_functions.send_alert(webhook_key,title,df,caption)
        logging.info("Sent Alert for CTA123")