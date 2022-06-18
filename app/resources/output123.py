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
    CTA1_1 =[]
    CTA1_2 =[]
    CTA1_3 =[]
    CTA1_4 =[]
    CTA2_1 =[] 
    CTA2_2 =[] 
    CTA2_3 =[] 
    CTA2_4 =[] 
    CTA3_1 =[]
    CTA3_2 =[]
    CTA3_3 =[]
    CTA3_4 =[]
    NESTED1 =[]
    NESTED2 =[]
    NESTED3 = []
    C3A1 =[]
    C3A2 =[]
    C3A3 = []
    string = []
    if len(df.index)>0:
        for index, row in df.iterrows():  
            if row['ActorModifiedby']  =='3BM1-20000-01':
                CTA1_1.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM1-20000-02':
                CTA1_2.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM1-20000-03':
                CTA1_3.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM1-20000-04':
                CTA1_4.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM2-20000-01':
                CTA2_1.append(f"{row['Thingname']}")    
            elif row['ActorModifiedby']  =='3BM2-20000-02':
                CTA2_2.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM2-20000-03':
                CTA2_3.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM2-20000-04':
                CTA2_4.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM3-20000-01':
                CTA3_1.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM3-20000-02':
                CTA3_2.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM3-20000-03':
                CTA3_3.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM3-20000-04':
                CTA3_4.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM1-29500-01':
                NESTED1.append(f"{row['Thingname']}") 
            elif row['ActorModifiedby']  =='3BM2-29500-01':
                NESTED2.append(f"{row['Thingname']}")
            elif row['ActorModifiedby']  =='3BM3-29500-01':
                NESTED3.append(f"{row['Thingname']}") 
            elif row['ActorModifiedby']  =='3BM1-40001-01':
                C3A1.append(f"{row['Thingname']}")  
            elif row['ActorModifiedby']  =='3BM2-40001-01':
                C3A2.append(f"{row['Thingname']}")  
            elif row['ActorModifiedby']  =='3BM3-40001-01':
                C3A3.append(f"{row['Thingname']}")

    string = [
              len(CTA1_1)/28, 
              len(CTA1_2)/28, 
              len(CTA1_3)/28, 
              len(CTA1_4)/28, 
              len(NESTED1)/4, 
              len(C3A1)/4, 
              len(CTA2_1)/28, 
              len(CTA2_2)/28, 
              len(CTA2_3)/28, 
              len(CTA2_4)/28, 
              len(NESTED2)/4 , 
              len(C3A2)/4, 
              len(CTA3_1)/28, 
              len(CTA3_2)/28, 
              len(CTA3_3)/28, 
              len(CTA3_4)/28, 
              len(NESTED3)/4 , 
              len(C3A3)/4]
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

    #Set outputs for table
    CTA1_1 = round(output_string[0],2)
    CTA1_2 = round(output_string[1],2)
    CTA1_3 = round(output_string[2],2)
    CTA1_4 = round(output_string[3],2)
    CTA2_1 = round(output_string[6],2)
    CTA2_2 = round(output_string[7],2)
    CTA2_3 = round(output_string[8],2)
    CTA2_4 = round(output_string[9],2)
    CTA3_1 = round(output_string[12],2)
    CTA3_2 = round(output_string[13],2)
    CTA3_3 = round(output_string[14],2)
    CTA3_4 = round(output_string[15],2)
    CTA1 = round(CTA1_1+CTA1_2+CTA1_3+CTA1_4,2)
    CTA2 = round(CTA2_1+CTA2_2+CTA2_3+CTA2_4,2)
    CTA3 = round(CTA3_1+CTA3_2+CTA3_3+CTA3_4,2)
    CTA_TOTAL = round(CTA1+CTA2+CTA3,2)

    MAMC1 = output_string[4]
    MAMC2 = output_string[10]
    MAMC3 = output_string[16]
    MANUAL_MAMC = df_sql_mmamc3['count(distinct tp.thingid)/4'][0]
    MAMC_TOTAL = MAMC1+MAMC2+MAMC3+MANUAL_MAMC

    C3A1 = output_string[5]
    C3A2 = output_string[11]
    C3A3 = output_string[17]
    C3A_TOTAL = C3A1+C3A2+C3A3

    # Setup teams output table
    title='BMA123 Hourly Update'
    payload={"title":title, 
            "summary":"summary",
        "sections":[
            {'text':f"""<table>
            <table>
                <tr>
                    <th>    </th>
                    <th style="text-align:center">BMA1</th>
                    <th style="text-align:center">BMA2</th>
                    <th style="text-align:center">BMA3</th>
                    <th style="text-align:center">MMAMC</th>
                    <th style="text-align:center">TOTAL</th>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>CTA</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA1)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA2)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA3)}</td>
                    <td style="text-align:center">----</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA_TOTAL)}</td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>MAMC</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(MAMC1)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(MAMC2)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(MAMC3)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(MANUAL_MAMC)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(MAMC_TOTAL)}</td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>C3A</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(C3A1)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(C3A2)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(C3A3)}</td>
                    <td style="text-align:center">----</td>
                    <td style="text-align:center">{'{:.2f}'.format(C3A_TOTAL)}</td>
                </tr>
                <tr bgcolor="#FFFFFF" height=10px></tr>
                <tr>
                    <td>    </td>
                    <td style="text-align:center"><strong>CTA1</strong></td>
                    <td style="text-align:center"><strong>CTA2</strong></td>
                    <td style="text-align:center"><strong>CTA3</strong></td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>LANE 1</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA1_1)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA2_1)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA3_1)}</td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>LANE 2</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA1_2)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA2_2)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA3_2)}</td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>LANE 3</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA1_3)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA2_3)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA3_3)}</td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>LANE 4</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA1_4)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA2_4)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA3_4)}</td>
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
