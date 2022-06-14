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
    bma4cta_o=df_bma4cta['count(distinct tp.thingid)/28'][0]
    bma4cta_o=round(bma4cta_o,2)
    logging.info("bma4cta end %s" % datetime.utcnow())

    #Grab BMA5-CTA hourly data
    sql_bma5cta=file_reader("resources/sql_queries/bma5cta_output.sql")
    sql_bma5cta=sql_bma5cta.format(start_time=start,end_time=end)
    df_bma5cta=db_connector(False,"MOS",sql=sql_bma5cta)
    df_bma5cta.fillna(0)
    bma5cta_o=df_bma5cta['count(distinct tp.thingid)/28'][0]
    bma5cta_o=round(bma5cta_o,2)
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
            <tr><th>BMA</th><th>CTA UPH</th><th>MAMC UPH</th><th>C3A UPH</th></tr>
            <tr><td>BMA4</td><td>{bma4cta_o}</td><td>{bma4mamc_o}</td><td>{bma4c3a_o}</td></tr>
            <tr><td>BMA5</td><td>{bma5cta_o}</td><td>{bma5mamc_o}</td><td>{bma5c3a_o}</td></tr>
            <tr><td><b>TOTAL</b></td><td>{bma4cta_o+bma5cta_o}</td><td>{bma4mamc_o+bma5mamc_o}</td><td>{bma4c3a_o+bma5c3a_o}</td></tr>
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

