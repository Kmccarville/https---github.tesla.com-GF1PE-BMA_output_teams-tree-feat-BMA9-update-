from common.db import db_connector
from common.stash_reader import stash_reader
from common.helper_creds import helper_creds
from common.helper_functions import file_reader

from datetime import datetime
from datetime import timedelta
import logging 
import requests
from requests.exceptions import Timeout
import json


testUrl = 'https://teslamotorsinc.webhook.office.com/webhookb2/8f75c3a4-3dde-4308-be4f-157c85688084@9026c5f4-86d0-4b9f-bd39-b7d4d0fb4674/IncomingWebhook/f229c49c229e4563b218df3f751aa116/6b1271fb-dfad-4abd-b25a-f204b0dbab0b'


def uph_calculation(df):
    logging.info("uph_calculation start %s" % datetime.utcnow())
    ACTA1  =[]
    ACTA2 =[] 
    ACTA3 =[]
    NESTED1 =[]
    NESTED2 =[]
    NESTED3 = []
    AC3A1 =[]
    AC3A2 =[]
    AC3A3 = []
    string = []
    if len(df.index)>0:
        for index, row in df.iterrows():  
            if row['CreatedBy']  =='ignition-gf1-bm-tag7-prod':
                ACTA1.append(f"{row['Thingname']}")
            elif row['CreatedBy']  =='ignition-gf1-bm-tag8-prod':
                ACTA2.append(f"{row['Thingname']}")
            elif row['CreatedBy']  =='ignition-gf1-bm-tag9-prod':
                ACTA3.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM1-29500-01':
                NESTED1.append(f"{row['Thingname']}") 
            elif row['ActorModifiedby']  =='3BM2-29500-01':
                NESTED2.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM3-29500-01':
                NESTED3.append(f"{row['Thingname']}") 
            elif row['ActorModifiedby']  =='3BM1-40001-01':
                AC3A1.append(f"{row['Thingname']}")  
            elif row['ActorModifiedby']  =='3BM2-40001-01':
                AC3A2.append(f"{row['Thingname']}")  
            elif row['ActorModifiedby']  =='3BM3-40001-01':
                AC3A3.append(f"{row['Thingname']}")                                

        
    string = [len(ACTA1)/28 ,len(NESTED1)/4, len(AC3A1)/4, len(ACTA2)/28 ,len(NESTED2)/4 , len(AC3A2)/4, len(ACTA3)/28, len(NESTED3)/4 , len(AC3A3)/4]
    string_format = [ round(elem,2) for elem in string ]
    logging.info("uph_calculation end %s" % datetime.utcnow())
    return(string_format)

def output123():
    logging.info("output123 start %s" % datetime.utcnow())
    lookback=1 #1 hr
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    #grab hourly bma123
    sql_bma123=file_reader("sql_queries/bma123_output.sql")
    sql_bma123=sql_bma123.format(start_time=start,end_time=end)
    df_bma123=db_connector(False,"MOS",sql=sql_bma123)
    df_bma123.fillna(0)
    output_string= uph_calculation(df_bma123)

    #grab hourly MMAMC 
    sql_mmamc3=file_reader("sql_queries/mmamc3_output.sql")

    # sql_c3a_53=f"""
    # SELECT count(distinct tp.thingid)/4 FROM thingpath tp
    # JOIN flowstep f ON tp.flowstepid = f.id
    # WHERE f.name in ('MBM-44000') AND tp.exitcompletioncode = 'PASS' AND tp.completed between '{start}' and '{end}'
    # """
    logging.info("mmamc3 start %s" % datetime.utcnow())
    df_sql_mmamc3=db_connector(False,"MOS",sql=sql_mmamc3)
    logging.info("mmamc3 end %s" % datetime.utcnow())
    #df_sql_c3a_53=db_connector(False,"MOS",sql=sql_c3a_53)
    df_sql_mmamc3.fillna(0)
    #df_sql_c3a_53.fillna(0)
    # print(df_sql_mmamc3)
    # print(df_sql_c3a_53)
    title='Hourly Summary'
    payload={"title":title, 
        "summary":"summary",
        "sections":[
            {'text':f"""<table>
            <tr><th>BMA</th><th>CTA UPH</th><th>MAMC UPH</th><th>C3A UPH</th></tr>
            <tr><td>BMA1</td><td> {output_string[0]}</td><td> {output_string[1]}</td><td> {output_string[2]}</td></tr>
            <tr><td>BMA2</td><td> {output_string[3]}</td><td> {output_string[4]}</td><td> {output_string[5]}</td></tr>
            <tr><td>BMA3</td><td> {output_string[6]}</td><td> {output_string[7]}</td><td> {output_string[8]}</td></tr>
            <tr><td>MMAMC3</td><td> 0 </td><td> {df_sql_mmamc3['count(distinct tp.thingid)/4'][0]} </td><td> 0 </td></tr>
            <tr><td><b>TOTAL</b></td><td>{round(output_string[0]+output_string[3]+output_string[6],2)}</td><td>{round(output_string[1]+output_string[4]+output_string[7]+df_sql_mmamc3['count(distinct tp.thingid)/4'][0],2)}</td><td>{round(output_string[2]+output_string[5]+output_string[8],2)}</td></tr>
            </table>""" }]}
    
    headers = {
    'Content-Type': 'application/json'
    }
   
    #post to BMA123-PE --> Output Channel
    if env=="prod":
        try:
            logging.info("BMA123 webhook start %s" % datetime.utcnow())
            response = requests.post(helper_creds.get_teams_webhook_BMA123()['url'], timeout=10, headers=headers, data=json.dumps(payload))
            logging.info("BMA123 webhook end %s" % datetime.utcnow())
            #requests.post(helper_creds.get_teams_webhook_MY3()['url'],headers=headers, data=json.dumps(payload))
            #logging.info("BMA123 MY3 webhook end %s" % datetime.utcnow())
        except Timeout:
            try:
                logging.info("RETRY BMA123 webhook start %s" % datetime.utcnow())
                response = requests.post(helper_creds.get_teams_webhook_BMA123()['url'], timeout=10, headers=headers, data=json.dumps(payload))
                logging.info("RETRY BMA123 webhook end %s" % datetime.utcnow())
            except Timeout:
                logging.info("BMA123 Webhook failed")
                pass
    else:
        try:
            response = requests.post(testUrl, timeout=10, headers=headers, data=json.dumps(payload))
        except Timeout:
            logging.info("BMA123 Webhook failed")
            pass
