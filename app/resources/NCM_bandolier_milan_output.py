#Auto Posting for Milan output - trigger around 7 PM everyday
from common import helper_functions 
import pandas as pd
from urllib.parse import quote
import sqlalchemy
import pymysql
import pymsteams
from datetime import datetime
from datetime import timedelta

def sendTeamsMessage(webhook, title, summary, message,color='#cc0000'):
    teams_msg = pymsteams.connectorcard(webhook)
    teams_msg.title(title)
    teams_msg.summary(summary)
    teams_msg.color(color)

    # create cards for each major html
    output_card = pymsteams.cardsection()
    output_card.text(message)
    teams_msg.addSection(output_card)

    # SEND text to Teams
    teams_msg.send()
    
def main(env):
    lookback=12
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)
    
    query = f"""SELECT DISTINCT
    t.created 'Bando_Created',
    t.name 'Bando_Serial',
    CONVERT_TZ(tp.exited, 'UTC', 'US/Pacific') 'Transaction_Time',
    tp.exitcompletioncode 'Exit_Reason',
    tp.flowstepname 'Flowstep_Name',
    a.name 'Actor'
FROM
    thingpath tp
        INNER JOIN
    thing t ON t.id = tp.thingid
        INNER JOIN
    actor a ON a.id = t.modifiedby
WHERE
    tp.flowstepname = '3BM-20104-NCM'
        AND tp.iscurrent = 0
        AND tp.exited > NOW() - INTERVAL {lookback} HOUR 
        AND tp.exitcompletioncode = 'PASS'
        AND a.name = 'Bando-Rework-Milan-01'"""
    
    db = helper_functions.get_sql_conn('mos_rpt2',schema="sparq")
    df = pd.read_sql(query,db)
    db.close()
    u1=df['Bando_Serial'].nunique()
    
    message = f"""<html>
            <tr>
                <td <td> Milan Output for the day : </td>
                <td <td style="color: #0000ff" > {u1}</td>
                <td <td> bandoliers </td>
            </tr>
            </html>"""
        
    webhook = 'teams_webhook_NCM_Bando_Milan_Update' if env == 'prod' else 'teams_webhook_DEV_Updates'
    creds = helper_functions.get_pw_json(webhook)
    webhookURL = creds['url'] 
    msg_title = 'NCM - Milan Update'
    msg_summary = "Daily Update"
    sendTeamsMessage(webhookURL,msg_title,msg_summary,message)
