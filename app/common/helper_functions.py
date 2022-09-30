import json
import requests
from requests.exceptions import Timeout
import sqlalchemy
from urllib.parse import quote
from datetime import timedelta
import pandas as pd

def file_reader(FilePath):
    with open(FilePath,"r") as f:
        contents = f.read()
        return contents

with open(r'/app/secrets/credentials') as f:
    pw_json = json.load(f)
    f.close()

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

def send_to_teams(webhook_key, title, html,retry=0):
    webhook_json = get_pw_json(webhook_key)
    webhook = webhook_json['url']
    payload=    {
                "title":title, 
                "summary":"summary",
                "sections":[{'text':html}]
                }
    headers = {
    'Content-Type': 'application/json'
    }
    try:
        requests.post(webhook,timeout=10,headers=headers, data=json.dumps(payload))
    except Timeout:
        if retry==1:
            requests.post(webhook,timeout=10,headers=headers, data=json.dumps(payload))

#parse dataframes for line-based value
def get_val(df,query_val,query_col,return_col):
    if len(df):
        sub_df = df.query(f"{query_col}=='{query_val}'")
        val = sub_df.iloc[0][return_col] if len(sub_df) else 0
    else:
        val = 0
    return val

def send_to_teams(webhook_key, title, html,retry=0):
    webhook_json = get_pw_json(webhook_key)
    webhook = webhook_json['url']
    payload=    {
                "title":title, 
                "summary":"summary",
                "sections":[{'text':html}]
                }
    headers = {
    'Content-Type': 'application/json'
    }
    try:
        requests.post(webhook,timeout=10,headers=headers, data=json.dumps(payload))
    except Timeout:
        if retry==1:
            requests.post(webhook,timeout=10,headers=headers, data=json.dumps(payload))

#pull starved/blocked states
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