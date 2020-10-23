import time
import schedule
from datetime import datetime
import error_handler
from test import debug as masterdebug
import logging 
import urllib3
from db import teams_webhook
from db import teams_webhook_45
from db import db_connector
import requests
import json
import pandas 
from stash_reader import bmaoutput
import stash_reader
from datetime import timedelta

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
    string_format = [ '%.2f' % elem for elem in string ]
    return(string_format)
   
def output():
    #grab hourly data
    sql=bmaoutput()
    df=db_connector(False,"MOS",sql=sql) 
    output_string= uph_calculation(df)
    print (output_string)
    
    title='Hourly Summary'
    #message='<h2>UPH</h2>'

    payload={"title":title, 
        "summary":"summary",
        "sections":[
            {'text':"""<table><tr><th>BMA</th><th>ACTA UPH</th><th>NESTED UPH</th><th>AC3A UPH</th></tr><tr><td>BMA1</td>
            <td> {o1}</td><td> {o2}</td><td> {o3}</td></tr><tr><td>BMA2</td><td> {o4}</td><td> {o5}</td><td> {o6}</td></tr></tr><td>BMA3</td><td> {o7}</td><td> {o8}</td><td> {o9}</td></tr></tr></table>""".format(o1=output_string[0], o2=output_string[1] ,
            o3=output_string[2], o4=output_string[3], o5=output_string[4], o6=output_string[5], o7=output_string[6], o8=output_string[7], o9=output_string[8]) }]}
 
    #post to BMA123-PE --> Output Channel
    response = requests.post(teams_webhook, json.dumps(payload))


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
    bma4cta_o=df_bma4cta['count(distinct tp.thingid)/28'][0]

    sql_bma5cta=stash_reader.bma5cta_output()
    sql_bma5cta=sql_bma4cta.format(start_time=start,end_time=end)
    df_bma5cta=db_connector(False,"MOS",sql=sql_bma5cta)
    bma5cta_o=df_bma5cta['count(distinct tp.thingid)/28'][0]
    
    sql_bma4mamc=stash_reader.bma4mamc_output()
    sql_bma4mamc=sql_bma4mamc.format(start_time=start,end_time=end)
    df_bma4mamc=db_connector(False,"MOS",sql=sql_bma4mamc)
    bma4mamc_o=df_bma4mamc['count(distinct tp.thingid)/4'][0]

    sql_bma5mamc=stash_reader.bma5mamc_output()
    sql_bma5mamc=sql_bma5mamc.format(start_time=start,end_time=end)
    df_bma5mamc=db_connector(False,"MOS",sql=sql_bma5mamc)
    bma5mamc_o=df_bma5mamc['count(distinct tp.thingid)/4'][0]

    sql_bma4c3a=stash_reader.bma4c3a_output()
    sql_bma4c3a=sql_bma4c3a.format(start_time=start,end_time=end)
    df_bma4c3a=db_connector(False,"MOS",sql=sql_bma4c3a)
    bma4c3a_o=df_bma4c3a['count(distinct tp.thingid)/4'][0]

    sql_bma5c3a=stash_reader.bma5c3a_output()
    sql_bma5c3a=sql_bma5c3a.format(start_time=start,end_time=end)
    df_bma5c3a=db_connector(False,"MOS",sql=sql_bma5c3a)
    bma5c3a_o=df_bma5c3a['count(distinct tp.thingid)/4'][0]
    
    title='BMA45 Hourly Update'
    html_table=f"""
    <table>
    <tr><th>BMA</th><th>CTA UPH</th><th>MAMC UPH</th><th>C3A UPH</th></tr>
    <tr><td>BMA4</td><td>{bma4cta_o}</td><td>{bma4mamc_o}</td><td>{bma4c3a_o}</td></tr>
    <tr><td>BMA5</td><td>{bma5cta_o}</td><td>{bma5mamc_o}</td><td>{bma5c3a_o}</td></tr>
    <tr><td><b>TOTAL</b></td><td>{bma4cta_o+bma5cta_o}</td><td>{bma4mamc_o+bma5mamc_o}</td><td>{bma4c3a_o+bma5c3a_o}</td></tr>
    </tr>
    </table>
    """
    payload={"title":title, 
        "summary":"summary",
        "sections":[
            {'text':html_table}]}
    #post to BMA123-PE --> Output Channel
    response = requests.post(teams_webhook_45, json.dumps(payload))

#output45()

def run_schedule():
    while 1:
        schedule.run_pending()
        time.sleep(1) 

if __name__ == '__main__':
    if debug==True:
        logging.info("serve_active")
    elif debug==False:
        schedule.every().hour.at(":00").do(output)
        schedule.every().hour.at(":01").do(output45)
        run_schedule()
        logging.info("serve_active")