from common import helper_functions 
import pandas as pd
from urllib.parse import quote
import sqlalchemy
import pymysql
import pymsteams

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
    #mos_con = helper_functions.get_sql_conn('mos_rpt2')
    query = f"""SELECT 
    pi.id AS Pick_item,
    cnt.tag AS Container,
    rt.id AS Route_ID,
    rt.name AS Route,
    CONVERT_TZ(pi.created, 'UTC', 'US/Pacific') AS Pickitem_Created_Time,
    CONVERT_TZ(pi.modified, 'UTC', 'US/Pacific') AS Pickitem_Modified_Time
FROM
    pickitem pi
        LEFT JOIN
    actor a ON pi.actorid = a.id
        JOIN
    container cnt ON pi.containerid = cnt.id
        LEFT JOIN
    route rt ON rt.id = pi.routeid
WHERE
    a.name = 'GF1-A3-SR02-AGVSPUR'
        AND pi.status = 'Released'
        AND cnt.tag LIKE '03TG1G60000%%'"""

    db = helper_functions.get_sql_conn('mos_rpt2',schema="sparq")
    df = pd.read_sql(query,db)
    db.close()

    #Manual_pack = df['Route'].value_counts()['Manual Infeed Through ASRS to Pack Line 2']
    Manual_pack = (df['Route'] == 'Manual Infeed Through ASRS to Pack Line 2').sum()
    BP6_PickItems1 = (df['Route'] == 'Module ASRS to Pack Line 2(BP6)').sum()
    BP6_PickItems2 = (df['Route'] == 'Module ASRS to Pack Line 2').sum()
    BP6_PickItems = BP6_PickItems1 + BP6_PickItems2
    Lift_A_Spur = (df['Route'] == 'Module Rack Empty Return from Config to AGV Spur').sum()
    Lift_B_Spur = (df['Route'] == 'Module Rack Empty Return from Storage to AGV Spur').sum()
    #Lift_A_Spur = df['Route'].value_counts()['Module Rack Empty Return from Config to AGV Spur']
    #Lift_A_Spur = Lift_A_Spur1 + Lift_A_Spur2
    Total_Counts = len(df)
    
    if Lift_A_Spur >= 20:
        color_S = 'red'
    elif 10<= Lift_A_Spur <20: 
        color_S = 'orange'
    else:
        color_S = 'green'
         
    if Lift_B_Spur >= 20:
        color_S = 'red'
    elif 10<= Lift_B_Spur <20: 
        color_LB = 'orange'
    else:
        color_LB = 'green'

    if Manual_pack >= 20:
        color_M = 'red'
    elif 10<= Manual_pack <20:
        color_M = 'orange'
    else:
        color_M = 'green'

    if BP6_PickItems >= 20:
        color_B = 'red'
    elif 10<= BP6_PickItems <20:
        color_B = 'orange'
    else:
        color_B = 'green'

    message = f"""<table>
                <tr>
                    <th style="text-align:center">ASRS Picks</th>
                    <th style="text-align:center">Pick Counts</th>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>NCM Spur Picks</strong></td>
                    <td style="text-align:center; color:{color_S}">{Lift_A_Spur}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>Lift B Picks</strong></td>
                    <td style="text-align:center; color:{color_LB}">{Lift_B_Spur}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>Manual-BP6 Picks</strong></td>
                    <td style="text-align:center; color:{color_M}">{Manual_pack}</td>
                </tr>
                <tr>                
                    <td style="text-align:left"><strong>BP6 Picks</strong></td>
                    <td style="text-align:center; color:{color_B}">{BP6_PickItems}</td>
                </tr>
                <tr>                
                    <td style="text-align:left"><strong>Total</strong></td>
                    <td style="text-align:center"><strong>{Total_Counts}</strong></td>
                </tr>
                </table>"""
    
    webhook = 'teams_webhook_AGV_NCM_Spur_Update' if env == 'prod' else 'teams_webhook_DEV_Updates'
    creds = helper_functions.get_pw_json(webhook)
    webhookURL = creds['url'] 
    msg_title = 'AGV Spur Update'
    msg_summary = "Hourly Update"
    sendTeamsMessage(webhookURL,msg_title,msg_summary,message)

