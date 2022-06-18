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
        CTA4_1 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-02'")
        CTA4_2 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-03'")
        CTA4_3 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-04'")
        CTA4_4 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-05'")
        CTA4_5 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-06'")
        CTA4_6 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-07'")
        CTA4_7 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-08'")
        CTA4_8 = sub_df.iloc[0][1] if len(sub_df) else 0
        CTA4_SUM=round(df_bma4cta['UPH'].sum(), 2)
    else:
        CTA4_1 = 0
        CTA4_2 = 0
        CTA4_3 = 0
        CTA4_4 = 0
        CTA4_5 = 0
        CTA4_6 = 0
        CTA4_7 = 0
        CTA4_8 = 0
        CTA4_SUM = 0
    logging.info("bma4cta end %s" % datetime.utcnow())

    #Grab BMA5-CTA hourly data
    sql_bma5cta=file_reader("resources/sql_queries/bma5cta_output.sql")
    sql_bma5cta=sql_bma5cta.format(start_time=start,end_time=end)
    df_bma5cta=db_connector(False,"MOS",sql=sql_bma5cta)
    df_bma5cta.fillna(0)
    if len(df_bma5cta):
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-02'")
        CTA5_2 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-03'")
        CTA5_3 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-04'")
        CTA5_4 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-05'")
        CTA5_5 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-06'")
        CTA5_6 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-07'")
        CTA5_7 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-08'")
        CTA5_8 = sub_df.iloc[0][1] if len(sub_df) else 0
        CTA5_SUM=round(df_bma5cta['UPH'].sum(), 2)
    else:
        CTA5_1 = 0
        CTA5_2 = 0
        CTA5_3 = 0
        CTA5_4 = 0
        CTA5_5 = 0
        CTA5_6 = 0
        CTA5_7 = 0
        CTA5_8 = 0
        CTA5_SUM = 0
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

    #Set outputs
    CTA_TOTAL= round(CTA4_SUM+CTA5_SUM,2)

    MAMC4 = bma4mamc_o
    MAMC5 = bma5mamc_o
    MAMC_TOTAL= MAMC4+MAMC5

    C3A4 = bma4c3a_o
    C3A5 = bma5c3a_o
    C3A_TOTAL = C3A4+C3A5

    # Setup teams output table
    title='BMA45 Hourly Update'
    payload={"title":title, 
        "summary":"summary",
        "sections":[
            {'text':f"""<table>
            <table>
                <tr>
                    <th>    </th>
                    <th style="text-align:center">BMA4</th>
                    <th style="text-align:center">BMA5</th>
                    <th style="text-align:center">TOTAL</th>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>CTA</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_SUM)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_SUM)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA_TOTAL)}</td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>MAMC</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(MAMC4)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(MAMC5)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(MAMC_TOTAL)}</td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>C3A</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(C3A4)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(C3A5)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(C3A_TOTAL)}</td>
                </tr>
                <tr bgcolor="#FFFFFF" height=10px></tr>
                <tr>
                    <td>    </td>
                    <td style="text-align:center"><strong>CTA4</strong></td>
                    <td style="text-align:center"><strong>CTA5</strong></td>
                </tr>
                <tr>
                   <td style="text-align:right"><strong>LANE 1</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_1)}</td>
                    <td style="text-align:center">----</td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>LANE 2</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_2)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_2)}</td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>LANE 3</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_3)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_3)}</td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>LANE 4</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_4)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_4)}</td>
                </tr>
                <tr>
                   <td style="text-align:right"><strong>LANE 5</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_5)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_5)}</td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>LANE 6</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_6)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_6)}</td>
                </tr>
                <tr>
                   <td style="text-align:right"><strong>LANE 7</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_7)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_7)}</td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>LANE 8</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_8)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_8)}</td>
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

