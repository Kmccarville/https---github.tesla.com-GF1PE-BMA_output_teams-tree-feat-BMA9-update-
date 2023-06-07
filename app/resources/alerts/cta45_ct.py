import common.helper_functions as helper_functions

from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import numpy as np

def main(env):
    lookback=3
    now=datetime.utcnow()

    #only run this script at hours 0,3,9,12,15,21
    pst_now = helper_functions.convert_from_utc_to_pst(now)
    if (pst_now.hour%3 == 0 and pst_now.hour not in [6,18]) or env=='dev':
        logging.info("CTA45 Alert %s" % datetime.utcnow())
        now_sub3=now+timedelta(hours=-lookback)
        start=now_sub3.replace(minute=00,second=00,microsecond=00)
        end=start+timedelta(hours=lookback)

        CTA_CT_PATHS = [
                    '[TSL053_CTR025]CT_Reporting/CoolingTubeLoad',
                    '[TSL053_CTR025_02]CT_Reporting/CoolingTubeLoad',
                    '[TSL053_CTR025_03]CT_Reporting/CoolingTubeLoad',
                    '[TSL053_CTR025_04]CT_Reporting/CoolingTubeLoad',
                    '[TSL053_CTR025_05]CT_Reporting/CoolingTubeLoad',
                    '[TSL053_CTR025_06]CT_Reporting/CoolingTubeLoad',
                    '[TSL053_CTR025_07]CT_Reporting/CoolingTubeLoad',
                    '[TSL053_CTR025_08]CT_Reporting/CoolingTubeLoad',
                    '[TSL063_CTR025_02]CT_Reporting/CoolingTubeLoad',
                    '[TSL063_CTR025_03]CT_Reporting/CoolingTubeLoad',
                    '[TSL063_CTR025_04]CT_Reporting/CoolingTubeLoad',
                    '[TSL063_CTR025_05]CT_Reporting/CoolingTubeLoad',
                    '[TSL063_CTR025_06]CT_Reporting/CoolingTubeLoad',
                    '[TSL063_CTR025_07]CT_Reporting/CoolingTubeLoad',
                    '[TSL063_CTR025_08]CT_Reporting/CoolingTubeLoad'
                    ]

        LOW_LIMIT = 4000
        OVER_CT_LIMIT = 35000
        PERCENT_LIMIT = 0.85
        NUM_CYCLES_LIMIT = 55

        path_list = ""
        for path in CTA_CT_PATHS:
            path_list += ("'" + path + "'" + ',')
            
        path_list = '(' + path_list.strip(',') + ')'
        query = f"""
                    SELECT 
                        e.name as EQPT,
                        ch.elapsed_time as CT_SEC
                    FROM
                        rno_eqtstatushistory_batterymodule.equipment e 
                        JOIN rno_eqtstatushistory_batterymodule.cycle_history ch on ch.equipment_id = e.id
                    WHERE 
                        e.source_tagpath in {path_list}
                    AND ch.timestamp BETWEEN '{start}' and '{end}'
                    AND ch.elapsed_time > {LOW_LIMIT}
                """

        plc_con = helper_functions.get_sql_conn('plc_db')
        df = pd.read_sql(query,plc_con)
        plc_con.close()

        df.loc[:,'EQPT_SPLIT'] = df['EQPT'].str.split('-')
        df.loc[:,'LINE'] = df['EQPT_SPLIT'].str[0].str[-1]
        df.loc[:,'LANE'] = df['EQPT_SPLIT'].str[-1].str.split('_').str[0].str.strip('0')
        df.loc[:,'CTA_ID'] = 'CTA' + df['LINE'] + ' Lane' + df['LANE']

        df.loc[:,'OVER_CT'] = np.where(df['CT_SEC'] > OVER_CT_LIMIT,1,0)
        df.loc[:,'GOOD_CT'] = np.where(df['CT_SEC'] <= OVER_CT_LIMIT,1,0)

        df_summary = df.groupby(['CTA_ID'])[['OVER_CT','GOOD_CT']].sum().reset_index()
        df_summary.loc[:,'PERCENT_GOOD'] = df_summary['GOOD_CT']/(df_summary['OVER_CT'] + df_summary['GOOD_CT'])
        df_summary.loc[:,'NUM_CYCLES'] = df_summary['OVER_CT'] + df_summary['GOOD_CT']

        df_alert = df_summary.query(f'PERCENT_GOOD < {PERCENT_LIMIT} and NUM_CYCLES > {NUM_CYCLES_LIMIT}')
        if len(df_alert):
            df_alert.loc[:,'PERCENT_GOOD'] = round(df_alert['PERCENT_GOOD']*100,1)
            
            df_html = df_alert[['CTA_ID','PERCENT_GOOD']]
            df_html.rename({"PERCENT_GOOD" : "% Good Cycle"},axis=1,inplace=True)
            webhook_key = 'teams_webhook_Zone1_Alerts' if env=='prod' else 'teams_webhook_DEV_Updates'
            title = 'CTA45 Tube Loading - Cycle Time Alert!'
            caption = 'Percent of Cycle Times Below 35s in the Last 3 Hours'
            helper_functions.send_alert(webhook_key,title,df_html,caption)
            logging.info("Sent Alert for CTA45")
        else:
            logging.info("No Alert for CTA45 CT")