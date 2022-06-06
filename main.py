from contextlib import nullcontext
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
    lookback=1 #1 hr
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    #grab hourly bma123
    sql_bma123=stash_reader.bma123_output()
    sql_bma123=sql_bma123.format(start_time=start,end_time=end)
    df_bma123=db_connector(False,"MOS",sql=sql_bma123)
    df_bma123.fillna(0)
    output_string= uph_calculation(df_bma123)

    #grab hourly MMAMC 
    sql_mmamc3=f"""
    SELECT count(distinct tp.thingid)/4 FROM thingpath tp
    WHERE tp.flowstepname = 'MBM-25000' AND tp.exitcompletioncode = 'PASS' AND tp.completed between '{start}' and '{end}'
    """

    # sql_c3a_53=f"""
    # SELECT count(distinct tp.thingid)/4 FROM thingpath tp
    # JOIN flowstep f ON tp.flowstepid = f.id
    # WHERE f.name in ('MBM-44000') AND tp.exitcompletioncode = 'PASS' AND tp.completed between '{start}' and '{end}'
    # """

    df_sql_mmamc3=db_connector(False,"MOS",sql=sql_mmamc3)
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
    #logging.info("made it to zone 4 output for the hour")

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

    sql_bmaZ4=stash_reader.bmaZ4_output()
    sql_bmaZ4=sql_bmaZ4.format(start_time=start,end_time=end)
    df_bmaZ4=db_connector(False,"MOS",sql=sql_bmaZ4)
    df_bmaZ4.fillna(0)

    outout_MC1=df_bmaZ4['UPH'][0]
    outout_MC2=df_bmaZ4['UPH'][1]
        
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
    #define global variables
    LINE_LIST = ['3BM1','3BM2','3BM3','3BM4','3BM5']
    WB_CT_DICT = {
                    '3BM1' : {'E3S':33,'E1':31,'E3L':37},
                    '3BM2' : {'E3S':33,'E1':31,'E3L':37},
                    '3BM3' : {'E3S':33,'E1':31,'E3L':37},
                    '3BM4' : {'E3S':32,'E1':0 ,'E3L':38},
                    '3BM5' : {'E3S':34,'E1':0 ,'E3L':0 }               
                    }
    INGRESS_PATHS = [
                    '[3BM01_50000_00]01/_OEE_Reporting/TSMs/InputStation',
                    '[3BM02_50000_00]02/_OEE_Reporting/TSMs/InputStation',
                    '[3BM03_50000_00]03/_OEE_Reporting/TSMs/InputStation',
                    '[3BM04_01_50000_00]04_01/_OEE_Reporting/TSMs/Ingress01_IngressTransfer',
                    '[3BM04_02_50000_00]04_02/_OEE_Reporting/TSMs/Ingress02_IngressTransfer',
                    '[3BM04_03_50000_00]04_03/_OEE_Reporting/TSMs/Ingress03_IngressTransfer',
                    '[3BM5-50100-01]TSL0091 Ingress/Main/TSM/LatchFaultReporting'
                    ]
    PO_PATHS = [
                '[3BM01_50000_00]01/_OEE_Reporting/TSMs/Packout1_Packout',
                '[3BM02_50000_00]02/_OEE_Reporting/TSMs/Packout1_Packout',
                '[3BM03_50000_00]03/_OEE_Reporting/TSMs/Packout1_Packout',
                '[3BM04_57000_01]_OEE_Reporting/TSMs/Main',
                '[3BM5-57000-00]PackoutLoad/TSM/LatchFaultReporting']
    #pull starved states
    def query_tsm_state(start, end, paths, s_or_b, reason=0):
        
        start_pad = start-timedelta(hours=12)
        reason_str = f"AND reason={reason}" if reason else ""
        s_b_str = f"AND esd.description = '{s_or_b}'"

        path_list = ""
        for path in paths:
            path_list += ("'" + path + "'" + ',')
            
        path_list = '(' + path_list.strip(',') + ')'
        path_str = f"AND e.source_tagpath in {path_list}"
        tsm_query = f"""
        SELECT 
        LINE,
        ROUND(SUM(timestampdiff(SECOND,start_date_time,end_date_time))/60,0) as Duration
        FROM
        (
            SELECT
            LINE,
            CASE 
                WHEN start_date_time < '{start}'
                THEN '{start}'
                ELSE start_date_time
                END as start_date_time,
            CASE 
                WHEN end_date_time > '{end}'
                THEN '{end}'
                ELSE end_date_time
                END as end_date_time
            FROM
                    (
                    SELECT 
                    CASE
                    WHEN left(e.source_tagpath,6) = '[3BM5-' THEN '3BM5'
                    ELSE REPLACE(right(left(e.source_tagpath,6),5),'0','')
                    END AS LINE,
                    esh.start_time as start_date_time,
                    CASE 
                    WHEN esh.end_time is NULL THEN '{end}'
                    ELSE esh.end_time 
                    END AS end_date_time
                    FROM rno_eqtstatushistory_batterymodule.equipment_state_history esh
                    JOIN rno_eqtstatushistory_batterymodule.equipment e on e.id = esh.equipment_id
                    JOIN rno_eqtstatushistory_batterymodule.equipment_state_definition esd on esd.state = esh.state
                    WHERE esh.start_time BETWEEN '{start_pad}' AND '{end}'
                    {s_b_str}
                    {path_str}
                    {reason_str}
                    ORDER BY start_time ASC) a
            WHERE start_date_time < '{end}'
            AND end_date_time > '{start}'
            ) b
            GROUP BY 1;
        """
        
        df= db_connector(False,"PLC",sql=tsm_query)
        return df
    #pull bonder actual and ideal cycle times
    def query_bonder_ct(start,end):
        
        ct_query = f"""
        SELECT
        left(a.name,4) as LINE,
        CASE
        WHEN p.partnumber in ('1091593-01-H','1609293-01-A','1609293-02-A') THEN 'E3S'
        WHEN p.partnumber in ('1091594-01-H','1609294-01-A','1609294-02-A') THEN 'E3L'
        WHEN p.partnumber in ('1609905-01-A','1609906-01-A','1609906-02-A') THEN 'E1'
        END AS MODEL,
        AVG(MINUTE(timediff(tp.completed,tp.started))) as CT,
        COUNT(MINUTE(timediff(tp.completed,tp.started))) as MOD_COUNT
        FROM sparq.thing t
        JOIN sparq.part p on p.id = t.partid
        JOIN sparq.thingpath tp on tp.thingid = t.id
        JOIN sparq.actor a on a.id = tp.actormodifiedby
        WHERE tp.flowstepname = '3BM-52000'
        AND tp.completed BETWEEN '{start}' and '{end}'
        GROUP BY 1,2
        """
        df= db_connector(False,"MOS",sql=ct_query)
        
        ct_df = pd.DataFrame({'LINE' : [], 'CT' : []})
        i_ct_df = pd.DataFrame({'LINE' : [], 'I_CT' : []})
        row = []
        for line in LINE_LIST:
            sub_df = df.query(f"LINE=='{line}'")
            mod_count = 0
            count_x_ct = 0
            count_x_ict = 0
            for row in sub_df.itertuples(False,'Tuples'):
                mod_count += row.MOD_COUNT
                ideal_ct = WB_CT_DICT[line][row.MODEL]
                count_x_ct += row.MOD_COUNT*row.CT
                count_x_ict += row.MOD_COUNT*ideal_ct
            if mod_count:
                avg_ct = round(count_x_ct/mod_count)
                avg_i_ct = round(count_x_ict/mod_count)
            else:
                avg_ct = 'null'
                avg_i_ct = 'null'
            row = [line,avg_ct]
            i_row = [line,avg_i_ct]
            ct_df.loc[len(ct_df)] = row
            i_ct_df.loc[len(i_ct_df)] = i_row
        
        return ct_df,i_ct_df
    #pull uph
    def query_uph(start,end):
        uph_query=f"""
        SELECT 
        left(a.name,4) as LINE,
        count(distinct tp.thingid)/4 as UPH 
        FROM sparq.thingpath tp
        JOIN sparq.actor a on a.id = tp.modifiedby
        WHERE tp.flowstepname = ('3BM-57000') AND tp.exitcompletioncode = 'PASS'
        AND tp.completed between  '{start}' and '{end}'
        and left(a.name,3) =  '3BM'
        group by 1
        """
        
        df=db_connector(False,"MOS",sql=uph_query)
        
        return df
    #parse dataframes for values
    def get_val(df,line):
        if len(df):
            sub_df = df.query(f"LINE=='{line}'")
            val = sub_df.iloc[0][1] if len(sub_df) else 0
        elif not list(df.columns):
            val = -99
        else:
            val = 0
        return val
    #generate html payload from dataframes
    def make_html_payload(main_df, total_output):
        start = """<table>"""

        header = "<tr>"
        for col in column_names:
            header += f"<th>{col}</th>"       
        header += "</tr>"
        
        data = ""
        for row in main_df.itertuples(False,'Tuples'):
            wip = int(row.STARVED_WIP)
            mtr = int(row.STARVED_MTR)
            wb_act = row.WB_ACTUAL_CT
            wb_ideal = row.WB_IDEAL_CT
            color_str = "color:red" if wb_act > wb_ideal else ""
            data += f"""
            <tr>
            <td style="text-align:center">{row.LINE}</td>
            <td style="text-align:center">{row.UPH}</td>
            <td style="text-align:center">{wip}</td>
            <td style="text-align:center">{mtr}</td>
            <td style="text-align:center;{color_str}">{wb_act}</td>
            <td style="text-align:center">{wb_ideal}</td>
            </tr>
            """
            
        end = f"<tr><td><b>TOTAL</b></td><td>{total_output}</td></tr>  </table>"
            
        return start+header+data+end
    uph_df = query_uph(start,end)
    try:
        ing_df = query_tsm_state(start,end,INGRESS_PATHS,"Starved")
        po_df = query_tsm_state(start,end,PO_PATHS,"Starved",1)
    except:
        ing_df = pd.DataFrame({})
        po_df = pd.DataFrame({})
    wb_ct_df,wb_i_ct_df = query_bonder_ct(start,end)

    row = []
    all_dfs = [uph_df,ing_df,po_df,wb_ct_df,wb_i_ct_df]
    column_names = ['LINE','UPH','STARVED_WIP','STARVED_MTR','WB_ACTUAL_CT','WB_IDEAL_CT']
    main_df = pd.DataFrame({'LINE' : [], 'UPH' : [], 'STARVED_WIP' : [], 'STARVED_MTR' : [], 'WB_ACTUAL_CT' : [], 'WB_IDEAL_CT' : []})
    for line in LINE_LIST:
        row = [get_val(df,line) for df in all_dfs]
        row.insert(0,line)
        main_df.loc[len(main_df)] = row
    total_output = 0
    for row in main_df.itertuples(False,'Tuples'):
        total_output += row.UPH
    html_payload = make_html_payload(main_df,total_output)
    title='Zone 3 Hourly Update'
    payload={"title":title, 
            "summary":"summary",
            "sections":[{'text':html_payload}]}
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
        logging.info("Environment: %s", env)

        schedule.every().hour.at(":00").do(output)
        schedule.every().hour.at(":01").do(output45)
        schedule.every().hour.at(":02").do(outputz4)
        schedule.every().hour.at(":03").do(outputz3)

        if env == "dev":
            logging.info("Run all command executed")
            schedule.run_all(delay_seconds=10)
            logging.info("Run all command complete")

        logging.info("Hourly run schedule initiated")
        run_schedule()
