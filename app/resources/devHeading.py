import logging
import os
import traceback
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pymsteams
from common import helper_functions

branchName=os.getenv('ENVVAR1')
commit=os.getenv('ENVVAR2')

def main(start=True):
    logging.info("Send Dev Heading")

    #create bma header
    header_html = f"""<tr>
            <th style="text-align:center"></th>
            <th style="text-align:left">Details</th>
            </tr>
    """
    #create mamc output row
    branchName_html = f"""<tr>
            <td style="text-align:center"><strong>BranchName</strong></td>
            <td style="text-align:left">{branchName}</td>
            </tr>
    """
    #create c3a output row
    commit_html = f"""<tr>
            <td style="text-align:center"><strong>Commit</strong></td>
            <td style="text-align:left">{commit}</td>
            </tr>
    """

    #create full bma html with the above htmls
    output_html = '<table>' + header_html + branchName_html + commit_html + '</table>'

    #get webhook based on environment
    webhook_key = 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']

    #start end of shift message
    teams_msg = pymsteams.connectorcard(webhook)
    title = 'BMA Output Teams Build Triggered' if start==True else "BMA Output Teams Build Completed Successfully"
    teams_msg.title(title)
    teams_msg.summary('summary')
    TESLA_RED = '#cc0000'
    msg_color = TESLA_RED
    teams_msg.color(msg_color)

    #create cards for each major html
    output_card = pymsteams.cardsection()
    output_card.text(output_html)
    teams_msg.addSection(output_card)

    #SEND IT
    try:
        teams_msg.send()
    except pymsteams.TeamsWebhookException:
        logging.warn("Webhook timed out, retry once")
        try:
            teams_msg.send()
        except pymsteams.TeamsWebhookException:
            logging.exception("Webhook timed out twice -- pass to next area")
