import json
import time
import traceback
import sqlalchemy
from sqlalchemy import text
from urllib.parse import quote
from datetime import timedelta
import pandas as pd
import pytz
from datetime import datetime
import pymsteams
import logging

#email sender libs
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders


def file_reader(FilePath):
    with open(FilePath,"r") as f:
        contents = f.read()
        return contents

try:
    with open(r'/app/secrets/credentials') as f:
        pw_json = json.load(f)
        f.close()
except:
    with open('app\local_creds.json') as f:
        pw_json = json.load(f)
        pw_json = pw_json['credentials']
        f.close()
    pass

def get_pw_json(key):
    return pw_json[key]

def get_sql_conn(key, schema=None):
    cred = get_pw_json(key)
    # Pull database credentials
    user = cred['user']
    password = quote(cred['password'])
    hostname = cred['host']
    port = cred['port']

    schema_str = f"/{schema}" if schema else ""
    # Define database connection
    engine = sqlalchemy.create_engine(f'mysql+pymysql://{user}:{password}@{hostname}:{port}{schema_str}')
    # Return connection to engine
    return engine.connect()

def is_it_eos_or_24():
    #this should take care of DST
    pst = pytz.timezone('US/Pacific')
    utc = pytz.timezone('UTC')
    utc_now=utc.localize(datetime.utcnow())
    pst_now = utc_now.astimezone(pst)
    it_is_eos = True if pst_now.hour in [6,18] else False
    it_is_24 = True if pst_now.hour==6 else False
    return it_is_eos,it_is_24

def get_shift_and_date():
    #this should take care of DST
    pst = pytz.timezone('US/Pacific')
    utc = pytz.timezone('UTC')
    utc_now=utc.localize(datetime.utcnow())
    pst_now = utc_now.astimezone(pst)

    # move shift time to 0-11/12-23. Lookback always previous hour
    shiftStart = pst_now+timedelta(hours=-7)
    shiftDate = shiftStart.strftime('%Y-%m-%d')
    shiftWeekNum = shiftStart.isocalendar()[1]
    shiftDay = shiftStart.weekday()
    shiftHour = shiftStart.hour
    # Day: Sunday is 6 Monday is 0
    if shiftDay in (6,0,1):
        shift = 'A' if shiftHour in range(0,12) else 'C'
    elif shiftDay in (3,4,5):
        shift = 'B' if shiftHour in range(0,12) else 'D'
    elif shiftDay == 2:
        if shiftWeekNum%2 == 1:
            shift = 'A' if shiftHour in range(0,12) else 'C'
        else:
            shift = 'B' if shiftHour in range(0,12) else 'D'
    return shift,shiftDate

#parse dataframes for line-based value
def get_val(df,query_val,query_col,return_col):
    if len(df):
        sub_df = df.query(f"{query_col}=='{query_val}'")
        val = sub_df.iloc[0][return_col] if len(sub_df) else 0
    else:
        val = 0
    return val

#parse dataframes for line-based value
def get_val_2(df,query_val,query_col,query_val2,query_col2,return_col):
    if len(df):
        sub_df = df.query(f"{query_col}=='{query_val}' and {query_col2}=='{query_val2}'")
        val = sub_df.iloc[0][return_col] if len(sub_df) else 0
    else:
        val = 0
    return val

#small helper function to get output by line/flowstep and divides to get carset value
def get_output_val(df,flowstep,line=None,actor=None):
    if actor:
        df_sub = df.query(f"LINE=='{line}' and ACTOR=='{actor}' and FLOWSTEP=='{flowstep}'")
    elif line:
        df_sub = df.query(f"LINE=='{line}' and FLOWSTEP=='{flowstep}'")
    else:
        df_sub = df.query(f"FLOWSTEP=='{flowstep}'")
        
    if len(df_sub):
        return df_sub['OUTPUT'].sum()
    else:
        return 0

def get_flowstep_outputs(db,start,end,flowsteps):
    #create flowstep string based on flowstep list given
    flowstep_str = ""
    for flow in flowsteps:
        flowstep_str += f"'{flow}',"
    flowstep_str = flowstep_str[:-1]
    df = pd.DataFrame({})
    while start < end:
        start_next = start + timedelta(minutes=60)
        query = f"""
                SELECT 
                a.name as ACTOR, 
                left(a.name,IF(left(tp.flowstepname,2) = 'MC', 3, 4)) as LINE,
                tp.flowstepname as FLOWSTEP,
                COUNT(DISTINCT tp.thingid) as OUTPUT
                FROM
                sparq.thingpath tp 
                JOIN sparq.actor a on a.id = tp.actormodifiedby
                WHERE 
                tp.flowstepname IN ({flowstep_str})
                AND
                tp.completed between '{start}' and '{start_next}'
                AND tp.exitcompletioncode = IF(tp.flowstepname='3BM-40001','NO_INSPECT','PASS')
                GROUP BY 1,2,3
                """
        df_sub = pd.read_sql(query,db)
        df = pd.concat([df,df_sub],axis=0)
        start += timedelta(minutes=60)
        
    return df

#pull starved/blocked states
def query_tsm_state(db,start, end, paths, s_or_b, reason=0):
    start_pad = start-timedelta(hours=84) #go 7 days back to cover long states
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
    EQPT_NAME,
    SUM(timestampdiff(SECOND,start_date_time,end_date_time)) as Duration
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
                left(e.name,4) as LINE,
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

#pull starved/blocked states
def query_tsm_state_by_lane(db,start, end, paths, s_or_b, reason=0):
    start_pad = start-timedelta(hours=84) #go 7 days back to cover long states
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
    EQPT_NAME,
    SUM(timestampdiff(SECOND,start_date_time,end_date_time)) as Duration
    FROM
    (
        SELECT
        LINE,
        EQPT_NAME,
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
                left(e.name,4) as LINE,
                e.name as EQPT_NAME,
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
        GROUP BY 1,2;
    """
    
    df= pd.read_sql(tsm_query,db)
    return df

def query_tsm_cycle_time(db,start,end,paths,low_limit,high_limit):
    path_list = ""
    for path in paths:
        path_list += ("'" + path + "'" + ',')
        
    path_list = '(' + path_list.strip(',') + ')'
    query = f"""
                SELECT 
                CASE when left(e.name,5) = '3BM08' then '3BM8'
                   else left(e.name,4) end as LINE,
                    AVG(ch.elapsed_time) as CT_SEC
                FROM
                    rno_eqtstatushistory_batterymodule.equipment e 
                    JOIN rno_eqtstatushistory_batterymodule.cycle_history ch on ch.equipment_id = e.id
                WHERE 
                    e.source_tagpath in {path_list}
                AND ch.timestamp BETWEEN '{start}' and '{end}'
                AND ch.elapsed_time BETWEEN {low_limit} and {high_limit}
                GROUP BY 1
            """
    df = pd.read_sql(query,db)
    return df

def evaluate_record(db,name,hours,carsets):
    newRecord = False
    prevShift = str()
    prevDate = str()
    prevRecord = 0
    query = f"""
        SELECT 
            e.name,
            r.id,
            r.eqtid,
            r.hours,
            r.shift,
            r.date,
            r.carsets,
            r.recorded
        FROM
            records.records r
        INNER JOIN records.equipment e on e.id = r.eqtid
        WHERE
            e.name = '{name}'
            AND r.hours = {hours}
        ORDER BY carsets DESC
        LIMIT 1
    """
    df = pd.read_sql(text(query),db)
    if len(df):
        prevRecord = df.iloc[0]['carsets']
        prevShift = df.iloc[0]['shift']
        prevDate = df.iloc[0]['date']
        logging.info(f'Found previous record: {name} | {prevShift} | {prevDate} {prevRecord}')
        if carsets > prevRecord:
            newRecord = True
            shift,date = get_shift_and_date()
            if hours == 24 and shift == 'C':
                shift = 'AC'
            if hours == 24 and shift == 'D':
                shift = 'BD'
            logging.info(f'New Record Achieved: {name} | {hours} | {carsets}')
            try:
                df_insert = pd.DataFrame({
                                        'eqtid' : [df.iloc[0]['eqtid']],
                                        'hours' : [hours],
                                        'shift' : [shift],
                                        'date' : [date],
                                        'carsets' : [round(carsets,1)],
                                        'recorded' : [datetime.now()]
                                    })
                df_insert.to_sql('records',db,'records',if_exists='append',index=False)
                logging.info(f'Inserted new record to database')
            except:
                logging.exception("Failed to insert record to database")
    else:
        logging.error(f'No previous record found: {name} | {hours} | {carsets}')
    return newRecord, prevShift, prevDate, prevRecord

def convert_from_utc_to_pst(inp_time):
    pst = pytz.timezone('US/Pacific')
    utc = pytz.timezone('UTC')
    utc_time=utc.localize(inp_time)
    pst_time = utc_time.astimezone(pst)
    return pst_time

def send_alert(webhook_key,title,df,caption="",link_title="",link_button=""):
    webhook_json = get_pw_json(webhook_key)
    webhook = webhook_json['url']
    
    html = df.to_html(index=False,border=0,justify='left',bold_rows=True)
    
    teams_msg = pymsteams.connectorcard(webhook)
    teams_msg.title(title)
    teams_msg.summary('summary')
    
    card = pymsteams.cardsection()
    card.title(caption)
    card.text(html)
    teams_msg.addSection(card)

    if link_title != "" and link_button != "":
        teams_msg.addLinkButton(link_title,link_button)
    try:
        teams_msg.send()
    except pymsteams.TeamsWebhookException:
        logging.warn("Webhook timed out, retry once")
        try:
            teams_msg.send()
        except pymsteams.TeamsWebhookException:
            logging.exception("Webhook timed out twice -- pass to next area")

def send_mail(send_from, send_to, subject, message, format='',
              files=[], filenames=[],
              server='smtp-int.teslamotors.com', port=25):
    """Compose and send email with provided info and attachments.
    Args:
        send_from (str): from name
        send_to (list[str]): to name(s)
        subject (str): message title
        message (str): message body
        files (list[var]): list of files
        filenames (list[str]): list of filenames
        server (str): mail server host name
        port (int): port number (587 or 25)
    """
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    if format == None:
        msg.attach(MIMEText(message))
    else:
        msg.attach(MIMEText(message,format))

    for attachment,name in zip(files,filenames):
        part = MIMEBase('application', "octet-stream")
        timestr = time.strftime('%Y%m%d-%H%M%S')
        part.set_payload(attachment.getvalue())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        f'attachment; filename={name}')
        msg.attach(part)
    try:
        smtp = smtplib.SMTP(server, port)
        smtp.sendmail(send_from, send_to, msg.as_string())
        smtp.quit()
    except smtplib.SMTPException as e:
        logging.error(str(e))

def e_handler(e):
    '''
    Error Handler: Send email with traceback logging
    '''
    trace = traceback.format_exc()
    send_from = 'bma-pybot-alerts@tesla.com'
    send_to = ['mberlied@tesla.com']
    subject = '[ERROR] BMA Teams Pybot Firing'

    message = f'''
            Exception Summary : {e}
            Traceback Error: 
                {trace}
            '''
    try:
        send_mail(send_from,send_to,subject,message)
    except Exception:
        logging.exception("failed to send exception email")
