from common.db import db_connector
from common.helper_functions import file_reader
import common.helper_creds as helper_creds


from datetime import datetime
from datetime import timedelta
import logging
import requests
from requests.exceptions import Timeout
import json

def outputz4(env):
    logging.info("Z4 start %s" % datetime.utcnow())

    lookback=1 #1 hr
    now=datetime.utcnow() 
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    #grab Zone4 hourly 
    sql_bmaZ4=file_reader("resources/sql_queries/bmaZ4_output.sql")
    sql_bmaZ4=sql_bmaZ4.format(start_time=start,end_time=end)
    df_bmaZ4=db_connector(False,"MOS",sql=sql_bmaZ4)
    df_bmaZ4.fillna(0)
    logging.info("z4 query end %s" % datetime.utcnow())

    MC1_UPH=df_bmaZ4['UPH'][0]
    MC2_UPH=df_bmaZ4['UPH'][1]
    MIC_TOTAL = MC1_UPH+MC2_UPH

    # Setup teams output table
    title='Zone 4 Hourly Update'
    payload={"title":title, 
        "summary":"summary",
        "sections":[
            {'text':f"""<table>
            <tr>
                <th style="text-align:right">LINE</th>
                <th style="text-align:center">UPH</th>
            </tr>
            <tr>
                <td style="text-align:right"><strong>MC1</strong></td>
                <td <td style="text-align:center">{'{:.2f}'.format(MC1_UPH)}</td>
            </tr>
            <tr>
                <td style="text-align:right"><strong>MC2</strong></td>
                <td style="text-align:center">{'{:.2f}'.format(MC2_UPH)}</td>
            </tr>
            <tr>
                <td style="text-align:right"><strong>TOTAL</strong></td>
                <td style="text-align:center">{'{:.2f}'.format(MIC_TOTAL)}</td>
            </tr>
            </table>"""}]}

    #post to Zone4 --> Output Channel
    headers = {
    'Content-Type': 'application/json'
    }

    if env=="prod":
        try:
            logging.info("Z4 webhook start %s" % datetime.utcnow())
            response = requests.post(helper_creds.get_teams_webhook_Z4()['url'],timeout=10,headers=headers, data=json.dumps(payload))
            logging.info("Z4 webhook end %s" % datetime.utcnow())
        except Timeout:
            try:
                logging.info("RETRY Z4 webhook start %s" % datetime.utcnow())
                response = requests.post(helper_creds.get_teams_webhook_Z4()['url'],timeout=10,headers=headers, data=json.dumps(payload))
                logging.info("RETRY Z4 webhook end %s" % datetime.utcnow())
            except Timeout:
                logging.info("Z4 Webhook failed")
                pass
    else:
        try:
            response = requests.post(helper_creds.get_teams_webhook_DEV()['url'],timeout=10,headers=headers, data=json.dumps(payload))
        except Timeout:   
            logging.info("Z4 DEV Webhook failed")
            pass