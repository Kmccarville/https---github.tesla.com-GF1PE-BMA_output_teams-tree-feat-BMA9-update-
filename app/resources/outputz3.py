from common.db import db_connector
from common.helper_functions import file_reader
import common.helper_creds as helper_creds

from datetime import datetime
from datetime import timedelta
import logging
import requests
from requests.exceptions import Timeout
import pandas as pd
import json
import pytz
import warnings

warnings.filterwarnings("ignore")

#bonder ideal ct
def query_ideal_ct_data(db):
    query = """
            SELECT PRODUCTION_LINE AS LINE,UUT_MODEL,IDEAL_CYCLE_TIME/60 AS CT FROM m3_teep.ideal_cycle_times
            WHERE MACHINE_TYPE='BONDER'
            AND REVISION=(SELECT MAX(REVISION) FROM m3_teep.ideal_cycle_times WHERE MACHINE_TYPE='BONDER')
            """
    ct_df = pd.read_sql(query,db)
    return ct_df

def get_ideal_ct(ideal_ct_df,line,model):
    ct_query = ideal_ct_df.query(f"LINE=='{line}' and UUT_MODEL=='{model}'",engine='python')
    if len(ct_query):
        ct_ideal = ct_query.iloc[0]['CT']
    else:
        ct_ideal = 0
    return ct_ideal

#pull starved states
def query_tsm_state(db,start, end, paths, s_or_b, reason=0):
    
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
    
    df= pd.read_sql(tsm_query,db)
    return df

#pull bonder actual and ideal cycle times
def query_bonder_ct(db,start,end, ideal_ct_df):
    
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
    df = pd.read_sql(ct_query,db)
    
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
            ideal_ct = get_ideal_ct(ideal_ct_df,line,row.MODEL)
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
def query_uph(db,start,end):
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
    
    df = pd.read_sql(uph_query,db)
    
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
def make_html_payload(main_df, total_output,column_names):
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

def query_shift_output(db,shift_start,shift_end):
    df = pd.DataFrame({})
    while shift_start < shift_end:
        shift_start_next = shift_start + timedelta(minutes=60)
    
        uph_query=f"""
        SELECT 
        left(a.name,4) as LINE,
        count(distinct tp.thingid)/4 as OUTPUT 
        FROM sparq.thingpath tp
        JOIN sparq.actor a on a.id = tp.modifiedby
        WHERE tp.flowstepname = ('3BM-57000') AND tp.exitcompletioncode = 'PASS'
        AND tp.completed between  '{shift_start}' and '{shift_start_next}'
        and left(a.name,3) =  '3BM'
        group by 1
        """
        
        shift_start += timedelta(minutes=60)
        df_sub = pd.read_sql(uph_query,db)
        df = pd.concat([df,df_sub],axis=0)

    df_sum = df.groupby(['LINE'])['OUTPUT'].sum().reset_index()
    return df_sum

def get_shift_report_html(db_mos,db_plc,shift_end, ingress_paths, po_paths):
    shift_start = shift_end - timedelta(hours=12)
    logging.info(shift_start,shift_end)
    COLUMN_NAMES = ['LINE','OUTPUT','STARVED_WIP (MIN)', 'STARVED_MTR (MIN)']
    df_shift = query_shift_output(db_mos,shift_start,shift_end)
    ing_df = query_tsm_state(db_plc,shift_start,shift_end,ingress_paths,"Starved")
    po_df = query_tsm_state(db_plc,shift_start,shift_end,po_paths,"Starved",1)
    start = """<table>"""
    header = "<tr>"
    for col in COLUMN_NAMES:
        header += f"<th>{col}</th>"       
    header += "</tr>"

    data = ""
    total_output = 0
    for line in LINE_LIST:
        uph = get_val(df_shift,line)
        wip = int(get_val(ing_df,line))
        mtr = int(get_val(po_df,line))
        data += f"""
        <tr>
        <td style="text-align:center">{line}</td>
        <td style="text-align:center">{uph}</td>
        <td style="text-align:center">{wip}</td>
        <td style="text-align:center">{mtr}</td>
        </tr>
        """
        total_output += uph
        
    end = f"<tr><td><b>TOTAL</b></td><td>{total_output}</td></tr>  </table>"
    return start+header+data+end

def send_to_teams(webhook_key, title, html):
    webhook_json = helper_creds.get_pw_json(webhook_key)
    webhook = webhook_json['url']
    title='Zone 3 End of Shift'
    payload=    {
                "title":title, 
                "summary":"summary",
                "sections":[{'text':html}]
                }
    headers = {
    'Content-Type': 'application/json'
    }
    response = requests.post(webhook,timeout=10,headers=headers, data=json.dumps(payload))
    logging.info(response)

def outputz3(env):
    
    #begin by defining timestamps
    now=datetime.utcnow()
    logging.info("Z3 start %s" % now)
    lookback=1 #1 hr
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start_time=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    start_pst = start_time.astimezone(pytz.timezone('US/Pacific'))
    start_pst_str = start_pst.strftime("%Y-%m-%d %H:%M:%S")
    end_time=start_time+timedelta(hours=lookback)

    #define global variables
    LINE_LIST = ['3BM1','3BM2','3BM3','3BM4','3BM5']
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
                '[3BM04_50000]3BM05_57000/_OEE_Reporting/Packout_MTR']

    #establish db connections

    mos_con = helper_creds.get_sql_conn('mos_rpt2')
    plc_con = helper_creds.get_sql_conn('plc_db')
    ict_con = helper_creds.get_sql_conn('interconnect_ro')

    #get the uph of the hour
    uph_df = query_uph(mos_con,start_time,end_time)
    try:
        ing_df = query_tsm_state(plc_con,start_time,end_time,INGRESS_PATHS,"Starved")
        po_df = query_tsm_state(plc_con,start_time,end_time,PO_PATHS,"Starved",1)
    except:
        ing_df = pd.DataFrame({})
        po_df = pd.DataFrame({})
    
    ideal_ct_df = query_ideal_ct_data(ict_con)
    wb_ct_df,wb_i_ct_df = query_bonder_ct(mos_con,start_time,end_time,ideal_ct_df)
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

    html_payload = make_html_payload(main_df,total_output,column_names)
    title='Zone 3 Hourly Update'
    payload={"title":title, 
            "summary":"summary",
            "sections":[{'text':html_payload}]}
    headers = {
    'Content-Type': 'application/json'
    }

    # if start_pst.hour in [5,17]:
    if 1+1==2:
        shift_html = get_shift_report_html(mos_con,plc_con,end_time,INGRESS_PATHS, PO_PATHS)
        send_to_teams('teams_webhook_DEV_Updates', 'Zone 3 End of Shift', shift_html)

    mos_con.close()
    plc_con.close()
    ict_con.close()

    #post to Z3 Teams Channel --> Output Channel
    if env=="prod":
        try:
            logging.info("Z3 webhook start %s" % datetime.utcnow())
            requests.post(helper_creds.get_teams_webhook_Z3()['url'],timeout=10,headers=headers, data=json.dumps(payload))
            logging.info("Z3 webhook end %s" % datetime.utcnow())
        except Timeout:
            try:
                logging.info("RETRY Z3 webhook start %s" % datetime.utcnow())
                requests.post(helper_creds.get_teams_webhook_Z3()['url'],timeout=10,headers=headers, data=json.dumps(payload))
                logging.info("RETRY Z3 webhook end %s" % datetime.utcnow())
            except Timeout:
                logging.info("Z3 Webhook failed")
    
    else:
        try:
            response = requests.post(helper_creds.get_teams_webhook_DEV()['url'],timeout=10,headers=headers, data=json.dumps(payload))
        except Timeout:
            logging.info("Z3 DEV Webhook failed")