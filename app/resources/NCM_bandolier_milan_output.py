#Auto Posting for Milan output - trigger around 7 PM everyday
from common import helper_functions 
import pandas as pd
from urllib.parse import quote
import sqlalchemy
import pymysql
import pymsteams

def sendTeamsMessage(webhook, title, summary, message,color='#cc0000'):
   #url = "https://teslamotorsinc.webhook.office.com/webhookb2/b2ec31db-df85-4ad3-8439-cec233bde55f@9026c5f4-86d0-4b9f-bd39-b7d4d0fb4674/IncomingWebhook/480e3c8ca5114e3aae7530cf995e5557/eed4c4c5-7e42-4f3c-8e39-e69a6fda4754"
   #title = 'NCM - Milan Update'
   #summary = "Bandolier NCM "
   #color='#cc0000'
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
        AND DATE(tp.exited) = CURDATE()
        AND tp.exitcompletioncode = 'PASS'
        AND a.name = 'Bando-Rework-Milan-01'"""
      
    db = helper_functions.get_sql_conn('mos_rpt2',schema="sparq")
    df = pd.read_sql(query,db)
    db.close()
    u1=df['Bando_Serial'].nunique()
      message = f"""
               <html>
                   <tr>
                       <td <td> Milan Output for the day : </td>
                       <td <td style="color: #0000ff" > {u1}</td>
                       <td <td> bandoliers </td>
                   </tr>
               </html>"""
      
    webhook = 'teams_webhook_NCM_Bando_Milan_Update' if env == 'prod' else 'teams_webhook_DEV_Updates'
    creds = helper_functions.get_pw_json(webhook)
    webhookURL = creds['url'] 
    msg_title = 'AGV Spur Update'
    msg_summary = "Daily Update"
    sendTeamsMessage(webhookURL,msg_title,msg_summary,message) 
