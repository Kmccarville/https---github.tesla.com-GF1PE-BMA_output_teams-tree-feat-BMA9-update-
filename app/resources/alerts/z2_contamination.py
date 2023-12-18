import common.helper_functions as helper_functions

from datetime import datetime
from datetime import timedelta
from sqlalchemy import text

import logging
import pandas as pd
import numpy as np
import pymsteams

def get_contaminated_modules(threshold_count):
    mos_con = helper_functions.get_sql_conn('mos_rpt2',schema='sparq')
    query = f"""
    SELECT distinct
             left(a.name,4) as 'MAMC Actor', 
             t.name as 'Module Serial',
             CASE
		WHEN nc.description like '%%adhesive%%' THEN 'Adhesive'
        WHEN nc.description like '%%frax%%' THEN 'Fiberfrax'
        WHEN nc.description like '%%foreign%%' THEN 'Foreign Object'
        WHEN nc.description like '%%tape%%' THEN 'Tape'
        ELSE 'OTHER'
	END AS 'FOD Category',
			CASE
		WHEN nc.state like '%%CLOSED%%' AND nca.disposition like '%%repair%%' then 'This module has been repaired for this defect.'
        WHEN nc.state like '%%CLOSED%%' AND nca.disposition like '%%scrap%%' then 'This module has been scrapped for this defect.'
        WHEN nc.state like '%%CLOSED%%' AND nca.disposition like 'NO PROBLEM FOUND' then 'This module has been released without rework.'
        else 'Pending Review'
	END AS 'NC Disposition'
    
            FROM nc                
                inner join thing t
                on t.id = nc.thingid
                inner join actor a
                on a.id = t.actorcreatedby
                inner join thingpath tp
                on tp.modified = t.modified
                left join ncaction nca on nca.ncid = nc.id
            WHERE
                nc.symptom = 'COSMETIC/DAMAGE'
                    AND nc.subsymptom = 'CONTAMINATION/ DEBRIS'
                    AND nc.processname = '3BM-Module'
                    AND nc.created >= NOW() - INTERVAL 20 hour
                    and nc.description not like '%%max pull test%%'
                    and (nc.description like '%%foreign%%' or nc.description like '%%fiber%%' or nc.description like '%%tape%%' or nc.description like '%%adhesive%%' or nc.description like '%%glove%%')
            """
    # get df
    
    df = pd.read_sql(query,mos_con)
    mos_con.close()
    # print(df)

    count_3BM1 = 0
    tname_3BM1 = []
    concat_3BM1 = []
    ncdispo_3BM1 = []

    count_3BM2 = 0
    tname_3BM2 = []
    concat_3BM2 = []
    ncdispo_3BM2 = [] 

    count_3BM3 = 0
    tname_3BM3 = []
    concat_3BM3 = []
    ncdispo_3BM3 = []

    count_3BM8 = 0
    tname_3BM8 = []
    concat_3BM8 = []
    ncdispo_3BM8 = []

    for row in df.iterrows():
        if row[1][0] == '3BM1':
            count_3BM1 = count_3BM1 + 1
            tname_3BM1.append(row[1][1])
            concat_3BM1.append(row[1][2])
            ncdispo_3BM1.append(row[1][3])


        elif row[1][0] == '3BM2':
            count_3BM2 = count_3BM2 + 1
            tname_3BM2.append(row[1][1])
            concat_3BM2.append(row[1][2])
            ncdispo_3BM2.append(row[1][3])

        elif row[1][0] == '3BM3':
            count_3BM3 = count_3BM3 + 1
            tname_3BM3.append(row[1][1])
            concat_3BM3.append(row[1][2])
            ncdispo_3BM3.append(row[1][3])
        
        elif row[1][0] == '3BM8':
            count_3BM8 = count_3BM8 + 1
            tname_3BM8.append(row[1][1])
            concat_3BM8.append(row[1][2])
            ncdispo_3BM8.append(row[1][3])

    content_html = ""

    if count_3BM1 > threshold_count:
        for i in range(count_3BM1):
            content_html +=f"""
            <tr>
                <td style="text-align:center">3BM1</td>
                <td style="text-align:center">{tname_3BM1[i]}</td>
		<td style="text-align:center">{concat_3BM1[i]}</td>
        <td style="text-align:center">{ncdispo_3BM1[i]}</td>
            </tr>
        """
    if count_3BM2 > threshold_count:
        for i in range(count_3BM2):
            content_html +=f"""
            <tr>
                <td style="text-align:center">3BM2</td>
                <td style="text-align:center">{tname_3BM2[i]}</td>
		<td style="text-align:center">{concat_3BM2[i]}</td>
        <td style="text-align:center">{ncdispo_3BM2[i]}</td>
            </tr>
        """
    if count_3BM3 > threshold_count:
        for i in range(count_3BM3):
            content_html +=f"""
            <tr>
                <td style="text-align:center">3BM3</td>
                <td style="text-align:center">{tname_3BM3[i]}</td>
		<td style="text-align:center">{concat_3BM3[i]}</td>
        <td style="text-align:center">{ncdispo_3BM3[i]}</td>
            </tr>
        """
    if count_3BM8 > threshold_count:
        for i in range(count_3BM8):
            content_html +=f"""
            <tr>
                <td style="text-align:center">3BM8</td>
                <td style="text-align:center">{tname_3BM8[i]}</td>
		<td style="text-align:center">{concat_3BM8[i]}</td>
        <td style="text-align:center">{ncdispo_3BM8[i]}</td>
            </tr>
        """
    return content_html

def main(env, threshold_count = 0):

    header_html = """
                        <tr>
                        <th style="text-align:center"><strong>LINE</strong></th>
                        <th style="text-align:center"><strong>Thing Serial</strong></th>
			<th style="text-align:center"><strong>Contaminant Category</strong></th>
            <th style="text-align:center"><strong>NC Disposition</strong></th>
                        </tr>
                    """
    content_html = get_contaminated_modules(threshold_count)
    if content_html != "":
        message = "<table>" + "<caption>Line and List of Serials</caption>" + header_html + content_html + "</table>"
        webhook_key = 'teams_webhook_FOD_Alert' if env=='prod' else 'teams_webhook_DEV_Updates'
        webhook_json = helper_functions.get_pw_json(webhook_key)
        webhook = webhook_json['url']
        
        #making the hourly teams message
        teams_msg = pymsteams.connectorcard(webhook)
        title = 'FOD / Contamination Alert'
        teams_msg.title(title)
        teams_msg.summary('summary')
        K8S_BLUE = '#3970e4'
        TESLA_RED = '#cc0000'
        msg_color = TESLA_RED #if eos else K8S_BLUE
        teams_msg.color(msg_color)
        #make a card with the hourly data
        output_card = pymsteams.cardsection()
        output_card.text(message)
    
        # teams_msg.addSection(summary_card)
        teams_msg.addSection(output_card)
        teams_msg.addLinkButton("OCAP will be attached here week of 12/18." , "https://confluence.teslamotors.com/display/PRODENG/Battery+Module+Hourly+Update")

        #SEND IT
        try:
            teams_msg.send()
        except pymsteams.TeamsWebhookException:
            logging.warn("Webhook timed out, retry once")
            try:
                teams_msg.send()
            except pymsteams.TeamsWebhookException:
                logging.exception("Webhook timed out twice -- pass to next area")
