import logging
from datetime import datetime, timedelta

import pandas as pd
import pymsteams
from common import helper_functions
from common.constants import K8S_BLUE


def main(env):
    #define start and end time for the hour
    lookback=1
    now=datetime.utcnow()
    logging.info("Close NC Check start %s" % datetime.utcnow())
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    ACTOR_CLOSED_BY = 'ignition-gf1-bm-tag5-prod'

    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    query = f"""
            SELECT 
                nc.description as NC_DESCRIPTION, 
                count(distinct t.name) as NUM_CLOSED
            FROM sparq.nc
            inner join sparq.thing t on t.id = nc.thingid
            inner join sparq.actor a on a.id = t.actorcreatedby
            WHERE closedby = '{ACTOR_CLOSED_BY}'
            and nc.created BETWEEN '{start}' and '{end}'
            and nc.description not in ('3BM-29500:NMAMC Leak Test - Short', '3BM-29500:NMAMC Leak Test-5Sec', '3BM-29600:NMAMC Leak Retest','3BM-29500:NMAMC Hipot', 'Issues detected on dispensed adhesive bead and clamshell has to be scrapped.', 'Adhesive has timed out (cured outside module) and IC clamshell has to be scrapped')
            and autoclosed = 0
            GROUP BY 1
            """

    df = pd.read_sql(query,mos_con)
    mos_con.close()

    if len(df) > 0:
        msg = f"""<table>
                <caption>NCs Auto Closed By {ACTOR_CLOSED_BY}</caption>
                <tr>
                        <th style="text-align:center">NC Description</th>
                        <th style="text-align:center">Num Closed</th>
                </tr>
                """
        for row in df.itertuples(False,'Tuples'):
            nc_desc = row.NC_DESCRIPTION
            nc_count= row.NUM_CLOSED
            msg += f"""
                    <tr>
                    <td style="text-align:center">{nc_desc}</td>
                    <td style="text-align:center">{nc_count}</td>    
                    </tr>
                    """

        msg += "</table>"

        webhook_key = 'teams_webhook_auto_close_nc' if env=='prod' else 'teams_webhook_DEV_Updates'
        webhook_json = helper_functions.get_pw_json(webhook_key)
        webhook = webhook_json['url']

        #start end of shift message
        teams_msg = pymsteams.connectorcard(webhook)
        title = 'Auto Close NC Report'
        teams_msg.title(title)
        teams_msg.summary('summary')
        msg_color = K8S_BLUE
        teams_msg.color(msg_color)

        #create cards for each major html
        msg_card = pymsteams.cardsection()
        msg_card.text(msg)
        teams_msg.addSection(msg_card)
        
        ocap_card = pymsteams.cardsection()
        ocap_step1 = "1. Immediately follow the escalation path by reaching out to the respective Quality Technician on schedule."
        ocap_step2 = "2. If there is no response within 30 mins, email a screenshot of the alert mentioning the details to M3M_Quality_Leadership@tesla.com"
        ocap_msg = """<body><p1 style="text-align:left">""" + ocap_step1 + "<br>" + ocap_step2 + "</p1></body>"
        ocap_card.text(ocap_msg)
        teams_msg.addSection(ocap_card)
        #SEND IT
        teams_msg.addLinkButton("Quality Tech Schedule", "https://confluence.teslamotors.com/pages/viewpage.action?spaceKey=GIG&title=Quality+Tech+Rosters")
        #SEND IT
        try:
            teams_msg.send()
        except pymsteams.TeamsWebhookException:
            logging.warn("Webhook timed out, retry once")
            try:
                teams_msg.send()
            except pymsteams.TeamsWebhookException:
                logging.exception("Webhook timed out twice -- pass to next area")
