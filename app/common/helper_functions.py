import json
import sqlalchemy
from urllib.parse import quote
from datetime import timedelta
import pandas as pd
import pytz
from datetime import datetime

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

def is_it_eos():
    #this should take care of DST
    pst = pytz.timezone('US/Pacific')
    utc = pytz.timezone('UTC')
    utc_now=utc.localize(datetime.utcnow())
    pst_now = utc_now.astimezone(pst)
    if pst_now.hour in [6,18]:
        return True
    else:
        return False

#parse dataframes for line-based value
def get_val(df,query_val,query_col,return_col):
    if len(df):
        sub_df = df.query(f"{query_col}=='{query_val}'")
        val = sub_df.iloc[0][return_col] if len(sub_df) else 0
    else:
        val = 0
    return val

#small helper function to get output by line/flowstep and divides to get carset value
def get_output_val(df,line,flowstep,actor=None):
    if actor:
        df_sub = df.query(f"LINE=='{line}' and ACTOR=='{actor}' and FLOWSTEP=='{flowstep}'")
    else:
        df_sub = df.query(f"LINE=='{line}' and FLOWSTEP=='{flowstep}'")
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
    #if the time between start and end is more than 1 hour, loop through
    delta = (end-start).seconds/3600
    if delta > 1: 
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
                    JOIN sparq.actor a on a.id = tp.modifiedby
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

    else:
        query = f"""
                    SELECT 
                    a.name as ACTOR, 
                    left(a.name,IF(left(tp.flowstepname,2) = 'MC', 3, 4)) as LINE,
                    tp.flowstepname as FLOWSTEP,
                    COUNT(DISTINCT tp.thingid) as OUTPUT
                    FROM
                    sparq.thingpath tp 
                    JOIN sparq.actor a on a.id = tp.modifiedby
                    WHERE 
                    tp.flowstepname IN ({flowstep_str})
                    AND
                    tp.completed between '{start}' and '{end}'
                    AND tp.exitcompletioncode = IF(tp.flowstepname='3BM-40001','NO_INSPECT','PASS')
                    GROUP BY 1,2,3
                    """
        df = pd.read_sql(query,db)
        
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