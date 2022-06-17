from common.db import db_connector
from common.helper_functions import file_reader
import common.helper_creds as helper_creds


from datetime import datetime
from datetime import timedelta
import logging
import requests
from requests.exceptions import Timeout
import json

def uph_calculation(df):
    logging.info("uph_calculation start %s" % datetime.utcnow())
    ACTA1_1 =[]
    ACTA1_2 =[]
    ACTA1_3 =[]
    ACTA1_4 =[]
    ACTA2_1 =[] 
    ACTA2_2 =[] 
    ACTA2_3 =[] 
    ACTA2_4 =[] 
    ACTA3_1 =[]
    ACTA3_2 =[]
    ACTA3_3 =[]
    ACTA3_4 =[]
    NESTED1 =[]
    NESTED2 =[]
    NESTED3 = []
    AC3A1 =[]
    AC3A2 =[]
    AC3A3 = []
    string = []
    if len(df.index)>0:
        for index, row in df.iterrows():  
            if row['ActorModifiedby']  =='3BM1-20000-01':
                ACTA1_1.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM1-20000-02':
                ACTA1_2.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM1-20000-03':
                ACTA1_3.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM1-20000-04':
                ACTA1_4.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM2-20000-01':
                ACTA2_1.append(f"{row['Thingname']}")    
            elif row['ActorModifiedby']  =='3BM2-20000-02':
                ACTA2_2.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM2-20000-03':
                ACTA2_3.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM2-20000-04':
                ACTA2_4.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM3-20000-01':
                ACTA3_1.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM3-20000-02':
                ACTA3_2.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM3-20000-03':
                ACTA3_3.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM3-20000-04':
                ACTA3_4.append(f"{row['Thingname']}")
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

    string = [len(ACTA1_1)/28, len(ACTA1_2)/28, len(ACTA1_3)/28, len(ACTA1_4)/28, len(NESTED1)/4, len(AC3A1)/4, len(ACTA2_1)/28, len(ACTA2_2)/28, len(ACTA2_3)/28, len(ACTA2_4)/28, len(NESTED2)/4 , len(AC3A2)/4, len(ACTA3_1)/28, len(ACTA3_2)/28, len(ACTA3_3)/28, len(ACTA3_4)/28, len(NESTED3)/4 , len(AC3A3)/4]
    string_format = [ round(elem,2) for elem in string ]
    logging.info("uph_calculation end %s" % datetime.utcnow())
    return(string_format)

def output123(env):
    logging.info("Output123 start %s" % datetime.utcnow())

    lookback=1 #1 hr
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    #Grab hourly bma123
    sql_bma123=file_reader("resources/sql_queries/bma123_output.sql")
    sql_bma123=sql_bma123.format(start_time=start,end_time=end)
    df_bma123=db_connector(False,"MOS",sql=sql_bma123)
    logging.info("bma123 sql end %s" % datetime.utcnow())
    df_bma123.fillna(0)
    output_string= uph_calculation(df_bma123)

    #Grab hourly MMAMC 
    sql_mmamc3=file_reader("resources/sql_queries/mmamc3_output.sql")
    sql_mmamc3=sql_mmamc3.format(start_time=start,end_time=end)
    df_sql_mmamc3=db_connector(False,"MOS",sql=sql_mmamc3)
    logging.info("mmamc3 end %s" % datetime.utcnow())
    df_sql_mmamc3.fillna(0)

    # Setup teams output table
    title='Hourly Summary'
    payload={"title":title, 
            "summary":"summary",
        "sections":[
            {'text':f"""<table>
            <table>
                <tr>
                    <th>Zone</th>
                    <th>BMA1</th>
                    <th>BMA2</th>
                    <th>BMA3</th>
                    <th>Total</th>
                </tr>
                <tr>
                    <td>CTA</td>
                    <td>{round(output_string[0]+output_string[1]+output_string[2]+output_string[3], 2)}</td>
                    <td>{round(output_string[6]+output_string[7]+output_string[8]+output_string[9], 2)}</td>
                    <td>{round(output_string[12]+output_string[13]+output_string[14]+output_string[15], 2)}</td>
                    <td>{round(output_string[0]+output_string[1]+output_string[2]+output_string[3]+output_string[6]+output_string[7]+output_string[8]+output_string[9]+output_string[12]+output_string[13]+output_string[14]+output_string[15], 2)}</td>
                </tr>
                <tr>
                    <td>MAMC</td>
                    <td>{output_string[4]}</td>
                    <td>{output_string[10]}</td>
                    <td>{output_string[16]}</td>
                    <td>{output_string[4]+output_string[10]+output_string[16]}</td>
                </tr>
                <tr>
                    <td>C3A</td>
                    <td>{output_string[5]}</td>
                    <td>{output_string[11]}</td>
                    <td>{output_string[17]}</td>
                    <td>{output_string[5]+output_string[11]+output_string[17]}</td>
                </tr>
                <tr bgcolor="#FFFFFF" height=10px></tr>
                <tr>
                    <td><strong>CTA_Lane</strong></td>
                    <td><strong>BMA1</strong></td>
                    <td><strong>BMA2</strong></td>
                    <td><strong>BMA3</strong></td>
                </tr>
                <tr>
                    <td>Lane 1</td>
                    <td>{output_string[0]}</td>
                    <td>{output_string[6]}</td>
                    <td>{output_string[12]}</td>
                </tr>
                <tr>
                    <td>Lane 2</td>
                    <td>{output_string[1]}</td>
                    <td>{output_string[7]}</td>
                    <td>{output_string[13]}</td>
                </tr>
                <tr>
                    <td>Lane 3</td>
                    <td>{output_string[2]}</td>
                    <td>{output_string[8]}</td>
                    <td>{output_string[14]}</td>
                </tr>
                <tr>
                    <td>Lane 4</td>
                    <td>{output_string[3]}</td>
                    <td>{output_string[9]}</td>
                    <td>{output_string[15]}</td>
                </tr>        
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
            logging.info("Start BMA123 webhook end %s" % datetime.utcnow())
            response = requests.post(helper_creds.get_teams_webhook_DEV()['url'], timeout=10, headers=headers, data=json.dumps(payload))
            logging.info("End BMA123 webhook end %s" % datetime.utcnow())
        except Timeout:
            logging.info("BMA123 Dev Webhook failed")
            pass
