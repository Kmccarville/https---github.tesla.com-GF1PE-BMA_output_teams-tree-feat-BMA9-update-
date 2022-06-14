from common.db import db_connector
# from common.stash_reader import stash_reader
import common.helper_creds
from datetime import datetime
from datetime import timedelta

import logging 
import requests
from requests.exceptions import Timeout
import json


def outputz4():
    #logging.info("made it to zone 4 output for the hour")
    logging.info("Z4 start %s" % datetime.utcnow())

    lookback=1 #1 hr
    now=datetime.utcnow() 
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    #grab hourly 
    sql=f"""
    SELECT left(f.name,3) as line,count(distinct tp.thingid)/4 as UPH FROM thingpath tp
    JOIN flowstep f ON tp.flowstepid = f.id
    WHERE f.name in ('MC1-30000','MC2-28000') AND tp.exitcompletioncode = 'PASS'
    AND tp.completed between '{start}' and '{end}'
    group by f.name
    """

    sql_bmaZ4=stash_reader.bmaZ4_output()
    sql_bmaZ4=sql_bmaZ4.format(start_time=start,end_time=end)
    df_bmaZ4=db_connector(False,"MOS",sql=sql_bmaZ4)
    df_bmaZ4.fillna(0)
    logging.info("z4 query end %s" % datetime.utcnow())

    outout_MC1=df_bmaZ4['UPH'][0]
    outout_MC2=df_bmaZ4['UPH'][1]
        
    title='Zone 4 Hourly Update'
    payload={"title":title, 
        "summary":"summary",
        "sections":[
            {'text':f"""<table>
            <tr><th>LINE</th><th>UPH</th></tr>
            <tr><td>MC1</td><td>{outout_MC1}</td></tr>
            <tr><td>MC2</td><td>{outout_MC2}</td></tr>
            <tr><td><b>TOTAL</b></td><td>{outout_MC1+outout_MC2}</td></tr>
            </table>"""}]}
    #post to BMA123-PE --> Output Channel
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
            response = requests.post(testUrl,timeout=10,headers=headers, data=json.dumps(payload))
        except Timeout:   
            logging.info("Z4 Webhook failed")
            pass

