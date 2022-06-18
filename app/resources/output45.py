from common.db import db_connector
from common.helper_functions import file_reader
import common.helper_creds as helper_creds


from datetime import datetime
from datetime import timedelta
import logging
import requests
from requests.exceptions import Timeout
import json
import os


def output45(env):
    logging.info("output45 start %s" % datetime.utcnow())
    lookback=1 #1 hr
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)
    
    #Grab BMA4-CTA hourly data
    sql_bma4cta=file_reader("resources/sql_queries/bma4cta_output.sql")
    sql_bma4cta=sql_bma4cta.format(start_time=start,end_time=end)
    df_bma4cta=db_connector(False,"MOS",sql=sql_bma4cta)
    df_bma4cta.fillna(0)
    if len(df_bma4cta):
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-01'")
        BMA4_1 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-02'")
        BMA4_2 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-03'")
        BMA4_3 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-04'")
        BMA4_4 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-05'")
        BMA4_5 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-06'")
        BMA4_6 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-07'")
        BMA4_7 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-08'")
        BMA4_8 = sub_df.iloc[0][1] if len(sub_df) else 0
        BMA4_SUM=round(df_bma4cta['UPH'].sum(), 2)
    else:
        BMA4_1 = 0
        BMA4_2 = 0
        BMA4_3 = 0
        BMA4_4 = 0
        BMA4_5 = 0
        BMA4_6 = 0
        BMA4_7 = 0
        BMA4_8 = 0
        BMA4_SUM = 0
    logging.info("bma4cta end %s" % datetime.utcnow())

    #Grab BMA5-CTA hourly data
    sql_bma5cta=file_reader("resources/sql_queries/bma5cta_output.sql")
    sql_bma5cta=sql_bma5cta.format(start_time=start,end_time=end)
    df_bma5cta=db_connector(False,"MOS",sql=sql_bma5cta)
    df_bma5cta.fillna(0)
    if len(df_bma5cta):
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-02'")
        BMA5_2 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-03'")
        BMA5_3 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-04'")
        BMA5_4 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-05'")
        BMA5_5 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-06'")
        BMA5_6 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-07'")
        BMA5_7 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-08'")
        BMA5_8 = sub_df.iloc[0][1] if len(sub_df) else 0
        BMA5_SUM=round(df_bma5cta['UPH'].sum(), 2)
    else:
        BMA5_1 = 0
        BMA5_2 = 0
        BMA5_3 = 0
        BMA5_4 = 0
        BMA5_5 = 0
        BMA5_6 = 0
        BMA5_7 = 0
        BMA5_8 = 0
        BMA5_SUM = 0
    logging.info("bam5cta end %s" % datetime.utcnow())

    #Grab BMA4-MAMC hourly data
    sql_bma4mamc=file_reader("resources/sql_queries/bma4mamc_output.sql")
    sql_bma4mamc=sql_bma4mamc.format(start_time=start,end_time=end)
    df_bma4mamc=db_connector(False,"MOS",sql=sql_bma4mamc)
    df_bma4mamc.fillna(0)
    bma4mamc_o=df_bma4mamc['count(distinct tp.thingid)/4'][0]
    logging.info("bma4mamc end %s" % datetime.utcnow())

    #Grab BMA5-MAMC hourly data
    sql_bma5mamc=file_reader("resources/sql_queries/bma5mamc_output.sql")
    sql_bma5mamc=sql_bma5mamc.format(start_time=start,end_time=end)
    df_bma5mamc=db_connector(False,"MOS",sql=sql_bma5mamc)
    df_bma5mamc.fillna(0)
    bma5mamc_o=df_bma5mamc['count(distinct tp.thingid)/4'][0]
    logging.info("bma5mamc_o end %s" % datetime.utcnow())

    #Grab BMA4-C3A hourly data
    sql_bma4c3a=file_reader("resources/sql_queries/bma4c3a_output.sql")
    sql_bma4c3a=sql_bma4c3a.format(start_time=start,end_time=end)
    df_bma4c3a=db_connector(False,"MOS",sql=sql_bma4c3a)
    df_bma4c3a.fillna(0)
    bma4c3a_o=df_bma4c3a['count(distinct tp.thingid)/4'][0]
    logging.info("bma4c3a_o end %s" % datetime.utcnow())

    #Grab BMA5-C3A hourly data
    sql_bma5c3a=file_reader("resources/sql_queries/bma5c3a_output.sql")
    sql_bma5c3a=sql_bma5c3a.format(start_time=start,end_time=end)
    df_bma5c3a=db_connector(False,"MOS",sql=sql_bma5c3a)
    df_bma5c3a.fillna(0)
    bma5c3a_o=df_bma5c3a['count(distinct tp.thingid)/4'][0]
    logging.info("bma5c3a end %s" % datetime.utcnow())

    # Setup teams output table
    title='BMA45 Hourly Update'
    payload={"title":title, 
        "summary":"summary",
        "sections":[
            {'text':f"""<table>
            <table>
                <tr>
                    <th>Zone</th>
                    <th>BMA4</th>
                    <th>BMA5</th>
                    <th>Total</th>
                </tr>
                <tr>
                    <td>CTA</td>
                    <td>{BMA4_SUM}</td>
                    <td>{BMA5_SUM}</td>
                    <td>{round(BMA4_SUM+BMA5_SUM,2)}</td>
                </tr>
                <tr>
                    <td>MAMC</td>
                    <td>{bma4mamc_o}</td>
                    <td>{bma5mamc_o}</td>
                    <td>{bma4mamc_o+bma5mamc_o}</td>
                </tr>
                <tr>
                    <td>C3A</td>
                    <td>{bma4c3a_o}</td>
                    <td>{bma5c3a_o}</td>
                    <td>{bma4c3a_o+bma5c3a_o}</td>
                </tr>
                <tr bgcolor="#FFFFFF" height=10px></tr>
                <tr>
                    <td><strong>CTA_Lane</strong></td>
                    <td><strong>BMA4</strong></td>
                    <td><strong>BMA5</strong></td>
                </tr>
                <tr>
                    <td>Lane 1</td>
                    <td>{BMA4_1}</td>
                    <td>----</td>
                </tr>
                <tr>
                    <td>Lane 2</td>
                    <td>{BMA4_2}</td>
                    <td>{BMA5_2}</td>
                </tr>
                <tr>
                    <td>Lane 3</td>
                    <td>{BMA4_3}</td>
                    <td>{BMA5_3}</td>
                </tr>
                <tr>
                    <td>Lane 4</td>
                    <td>{BMA4_4}</td>
                    <td>{BMA5_4}</td>
                </tr>
                <tr>
                    <td>Lane 5</td>
                    <td>{BMA4_5}</td>
                    <td>{BMA5_5}</td>
                </tr>
                <tr>
                    <td>Lane 6</td>
                    <td>{BMA4_6}</td>
                    <td>{BMA5_6}</td>
                </tr>
                <tr>
                    <td>Lane 7</td>
                    <td>{BMA4_7}</td>
                    <td>{BMA5_7}</td>
                </tr>
                <tr>
                    <td>Lane 8</td>
                    <td>{BMA4_8}</td>
                    <td>{BMA5_8}</td>
                </tr>
            </table>"""}]}
    headers = {
    'Content-Type': 'application/json'
    }

    #post to BMA45-PE --> Output Channel
    if env=="prod":
        try:
            logging.info("BMA45 webhook start %s" % datetime.utcnow())
            response = requests.post(helper_creds.get_teams_webhook_BMA45()['url'],timeout=10,headers=headers, data=json.dumps(payload))
            logging.info("BMA45 webhook end %s" % datetime.utcnow())
        except Timeout:
            try:
                logging.info("RETRY BMA45 webhook start %s" % datetime.utcnow())
                response = requests.post(helper_creds.get_teams_webhook_BMA45()['url'],timeout=10,headers=headers, data=json.dumps(payload))
                logging.info("RETRY BMA45 webhook end %s" % datetime.utcnow())
            except Timeout:
                logging.info("BMA45 Webhook failed")
                pass
    else:
        try:
            response = requests.post(helper_creds.get_teams_webhook_DEV()['url'],timeout=10,headers=headers, data=json.dumps(payload))
        except Timeout:
            logging.info("BMA123 Dev Webhook failed")
            pass

