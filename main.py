import time
import schedule
from datetime import datetime
import error_handler
from test import debug as masterdebug
import logging 
import urllib3
from db import teams_webhook
from db import db_connector
import requests
import json
import pandas 
from stash_reader import bmaoutput

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

#output()

def run_schedule():
    while 1:
        schedule.run_pending()
        time.sleep(1) 

if __name__ == '__main__':
    if debug==True:
        logging.info("serve_active")
    elif debug==False:
        schedule.every().hour.at(":00").do(output)
        logging.info("serve_active")