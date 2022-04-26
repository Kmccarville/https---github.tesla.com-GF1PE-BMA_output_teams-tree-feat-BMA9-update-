import time
import schedule
from datetime import datetime
import error_handler
from test import debug as masterdebug
import logging 
import urllib3
from db import db_connector
import requests
import json
import pandas as pd
from stash_reader import bmaoutput
import stash_reader
from datetime import timedelta
import helper_creds
import os

# test url is for a testing separate teams channel to debug without disruption to live team channel
testUrl = 'https://teslamotorsinc.webhook.office.com/webhookb2/8f75c3a4-3dde-4308-be4f-157c85688084@9026c5f4-86d0-4b9f-bd39-b7d4d0fb4674/IncomingWebhook/f229c49c229e4563b218df3f751aa116/6b1271fb-dfad-4abd-b25a-f204b0dbab0b'

logging.basicConfig(level=logging.INFO)
logging.info("main_active")
debug=masterdebug

def uph_calculation(df):
    
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
    return(string_format)
   
def output():
    #grab hourly data
    sql=bmaoutput()
    df=db_connector(False,"MOS",sql=sql) 
    df.fillna(0) # fills all sections with a 0 to be overwritten when pulled via db_connector, this avoids any null pointer errors
    # print('zone 1 df ' , df)
    output_string= uph_calculation(df)
    # print('zone 1 output string ' ,output_string)
    title='Hourly Summary'

    lookback=1 #1 hr
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)
    #grab hourly 
    sql_mamac_53=f"""
    SELECT count(distinct tp.thingid)/4 FROM thingpath tp
    JOIN flowstep f ON tp.flowstepid = f.id
    WHERE f.name in ('MBM-26000-02', 'MBM-27000-02') AND tp.exitcompletioncode = 'PASS' AND tp.completed between '{start}' and '{end}'
    """

    sql_c3a_53=f"""
    SELECT count(distinct tp.thingid)/4 FROM thingpath tp
    JOIN flowstep f ON tp.flowstepid = f.id
    WHERE f.name in ('MBM-44000') AND tp.exitcompletioncode = 'PASS' AND tp.completed between '{start}' and '{end}'
    """

    df_sql_mamac_53=db_connector(False,"MOS",sql=sql_mamac_53)
    df_sql_c3a_53=db_connector(False,"MOS",sql=sql_c3a_53)
    df_sql_mamac_53.fillna(0)
    df_sql_c3a_53.fillna(0)
    # print(df_sql_mamac_53)
    # print(df_sql_c3a_53)

    payload={"title":title, 
        "summary":"summary",
        "sections":[
            {'text':f"""<table>
            <tr><th>BMA</th><th>CTA UPH</th><th>MAMC UPH</th><th>C3A UPH</th></tr>
            <tr><td>BMA1</td><td> {output_string[0]}</td><td> {output_string[1]}</td><td> {output_string[2]}</td></tr>
            <tr><td>BMA2</td><td> {output_string[3]}</td><td> {output_string[4]}</td><td> {output_string[5]}</td></tr>
            <tr><td>BMA3</td><td> {output_string[6]}</td><td> {output_string[7]}</td><td> {output_string[8]}</td></tr>
            <tr><td>SA53</td><td> 0 </td><td> {df_sql_mamac_53['count(distinct tp.thingid)/4'][0]} </td><td> {df_sql_c3a_53['count(distinct tp.thingid)/4'][0]} </td></tr>
            <tr><td><b>TOTAL</b></td><td>{round(output_string[0]+output_string[3]+output_string[6],2)}</td><td>{round(output_string[1]+output_string[4]+output_string[7]+df_sql_mamac_53['count(distinct tp.thingid)/4'][0],2)}</td><td>{round(output_string[2]+output_string[5]+output_string[8]+df_sql_c3a_53['count(distinct tp.thingid)/4'][0],2)}</td></tr>
            </table>""" }]}
    
    headers = {
    'Content-Type': 'application/json'
    }
   
    #post to BMA123-PE --> Output Channel
    if env=="prod":
        response = requests.post(helper_creds.get_teams_webhook_BMA123()['url'],headers=headers, data=json.dumps(payload))
        requests.post(helper_creds.get_teams_webhook_MY3()['url'],headers=headers, data=json.dumps(payload))
    else:
         response = requests.post(testUrl,headers=headers, data=json.dumps(payload))
    print(response.text.encode('utf8'))
    
   

def output45():
    lookback=1 #1 hr
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)
    #grab hourly data
    sql_bma4cta=stash_reader.bma4cta_output()
    sql_bma4cta=sql_bma4cta.format(start_time=start,end_time=end)
    df_bma4cta=db_connector(False,"MOS",sql=sql_bma4cta)
    df_bma4cta.fillna(0)
    # print(df_bma4cta)
    bma4cta_o=df_bma4cta['count(distinct tp.thingid)/28'][0]

    sql_bma5cta=stash_reader.bma5cta_output()
    sql_bma5cta=sql_bma5cta.format(start_time=start,end_time=end)
    df_bma5cta=db_connector(False,"MOS",sql=sql_bma5cta)
    df_bma5cta.fillna(0)
    # print(df_bma5cta)
    bma5cta_o=df_bma5cta['count(distinct tp.thingid)/28'][0]
    
    sql_bma4mamc=stash_reader.bma4mamc_output()
    sql_bma4mamc=sql_bma4mamc.format(start_time=start,end_time=end)
    df_bma4mamc=db_connector(False,"MOS",sql=sql_bma4mamc)
    df_bma4mamc.fillna(0)
    # print( df_bma4mamc)
    bma4mamc_o=df_bma4mamc['count(distinct tp.thingid)/4'][0]

    sql_bma5mamc=stash_reader.bma5mamc_output()
    sql_bma5mamc=sql_bma5mamc.format(start_time=start,end_time=end)
    df_bma5mamc=db_connector(False,"MOS",sql=sql_bma5mamc)
    df_bma5mamc.fillna(0)
    # print(df_bma5mamc)
    bma5mamc_o=df_bma5mamc['count(distinct tp.thingid)/4'][0]

    sql_bma4c3a=stash_reader.bma4c3a_output()
    sql_bma4c3a=sql_bma4c3a.format(start_time=start,end_time=end)
    df_bma4c3a=db_connector(False,"MOS",sql=sql_bma4c3a)
    df_bma4c3a.fillna(0)
    # print(df_bma4c3a)
    bma4c3a_o=df_bma4c3a['count(distinct tp.thingid)/4'][0]

    sql_bma5c3a=stash_reader.bma5c3a_output()
    sql_bma5c3a=sql_bma5c3a.format(start_time=start,end_time=end)
    df_bma5c3a=db_connector(False,"MOS",sql=sql_bma5c3a)
    df_bma5c3a.fillna(0)
    # print(df_bma5c3a)
    bma5c3a_o=df_bma5c3a['count(distinct tp.thingid)/4'][0]


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
    #post to BMA123-PE --> Output Channel
    if env=="prod":
        response = requests.post(helper_creds.get_teams_webhook_BMA45()['url'],headers=headers, data=json.dumps(payload))
        requests.post(helper_creds.get_teams_webhook_MY3()['url'],headers=headers, data=json.dumps(payload))
    else: 
        response = requests.post(testUrl,headers=headers, data=json.dumps(payload))

def outputz4():
    logging.info("made it to zone 4 output for the hour")

    lookback=1 #1 hr
    now=datetime.utcnow() 
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    #grab hourly 
    sql=f"""
    SELECT left(f.name,3) as line,count(distinct tp.thingid)/4 as UPH FROM thingpath tp
    JOIN flowstep f ON tp.flowstepid = f.id
    WHERE f.name in ('MC1-30000','MC2-28000') AND tp.exitcompletioncode = 'PASS'
    AND tp.completed between '{start}' and '{end}'
    group by f.name
    """
    
    df=db_connector(False,"MOS",sql=sql)
    # print(df)
    payload={"title":"connection to db_connector is made"}
    df.fillna(0)

    outout_MC1=df['UPH'][0]
    outout_MC2=df['UPH'][1]
        
    title='Zone 4 Hourly Update'
    payload={"title":title, 
        "summary":"summary",
        "sections":[
            {'text':f"""<table>
            <tr><th>LINE</th><th>UPH</th></tr>
            <tr><td>MC1</td><td>{outout_MC1}</td></tr>
            <tr><td>MC2</td><td>{outout_MC2}</td></tr>
            <tr><td><b>TOTAL</b></td><td>{outout_MC1+outout_MC2}</td></tr>
            </table>"""}]}
    #post to BMA123-PE --> Output Channel
    headers = {
    'Content-Type': 'application/json'
    }
    
    if env=="prod":
        response = requests.post(helper_creds.get_teams_webhook_Z4()['url'],headers=headers, data=json.dumps(payload))
        requests.post(helper_creds.get_teams_webhook_MY3()['url'],headers=headers, data=json.dumps(payload))
    else:
        response = requests.post(testUrl,headers=headers, data=json.dumps(payload))   

def outputz3():
 
    lookback=1 #1 hr
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)
            
    #grab hourly 
    sql=f"""

    SELECT left(a.name,4) as line,count(distinct tp.thingid)/4 as UPH 
    FROM thingpath tp
    JOIN actor a on a.id = tp.modifiedby
    WHERE tp.flowstepname = ('3BM-57000') AND tp.exitcompletioncode = 'PASS'
    AND tp.completed between  '{start}' and '{end}'
    and a.name like '3BM%%'
    group by a.name
    """
    
    df=db_connector(False,"MOS",sql=sql)
    # print('**df before 0',df)
    payload={"title":"connection to db_connector is made"}
    df.fillna(0) 
    # print('**df after 0 filled',df)
    
    outout_BMA1= df['UPH'][0]
    outout_BMA2=df['UPH'][1]
    outout_BMA3=df['UPH'][2]
    outout_BMA4=df['UPH'][3]
    outout_BMA5=df['UPH'][4] 


    title='Zone 3 Hourly Update'
    payload={"title":title, 
        "summary":"summary",
        "sections":[
            {'text':f"""<table>
            <tr><th>LINE</th><th>UPH</th></tr>
            <tr><td>3BM1</td><td>{outout_BMA1}</td></tr>
            <tr><td>3BM2</td><td>{outout_BMA2}</td></tr>
            <tr><td>3BM3</td><td>{outout_BMA3}</td></tr>
            <tr><td>3BM4</td><td>{outout_BMA4}</td></tr>
            <tr><td>3BM5</td><td>{outout_BMA5}</td></tr>
            <tr><td><b>TOTAL</b></td><td>{outout_BMA1+outout_BMA2+outout_BMA3+outout_BMA4+outout_BMA5}</td></tr>
            </table>"""}]}
    headers = {
    'Content-Type': 'application/json'
    }
    #post to BMA123-PE --> Output Channel
    if env=="prod":
        requests.post(helper_creds.get_teams_webhook_Z3()['url'],headers=headers, data=json.dumps(payload))
        requests.post(helper_creds.get_teams_webhook_MY3()['url'],headers=headers, data=json.dumps(payload))
    
    else:   
        response = requests.post(testUrl,headers=headers, data=json.dumps(payload))

    
#output()
#outputz3()
#outputz4()
#output45()

def run_schedule():
    while 1:
        schedule.run_pending()
        time.sleep(1) 

if __name__ == '__main__':
    if debug==True:
        logging.info("serve_active")
        output()
  
    elif debug==False:
        env=os.getenv('ENVVAR3')

        logging.info("Code is running...better go catch it!")
        schedule.every().hour.at(":00").do(output)
        schedule.every().hour.at(":01").do(output45)
        schedule.every().hour.at(":02").do(outputz4)
        schedule.every().hour.at(":03").do(outputz3)
        run_schedule()
        logging.info.info("serve_active")
        
