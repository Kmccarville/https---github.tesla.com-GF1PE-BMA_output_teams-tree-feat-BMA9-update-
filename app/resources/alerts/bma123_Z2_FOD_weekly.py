import common.helper_functions as helper_functions

from datetime import datetime
from datetime import timedelta
from sqlalchemy import text

import logging
import pandas as pd
import numpy as np

def get_bypassed_table():
    LINE = [0]
    count = []
    mos_con = helper_functions.get_sql_conn('mos_db')
    query = f"""
       SELECT 
distinct left(a.name,4) as 'MAMC Actor', nc.thingname, nc.state as 'NC State', nc.createdby as 'NC CreatedBy', nc.detectedatstep as 'FOD caught at', nc.modified as 'NC modified', nca.disposition as 'NC Disposition', t.state as 'Module State', t.created as thingCreated
FROM
   nc force index (ix_nc_processname_created)
   inner join thing t
   on t.id = nc.thingid
   inner join actor a
   on a.id = t.actorcreatedby
   left join ncaction nca on nca.ncid = nc.id
 WHERE
  nc.symptom = 'COSMETIC/DAMAGE'
  AND nc.subsymptom = 'CONTAMINATION/ DEBRIS'
  AND nc.processname = '3BM-Module'
  AND nc.created >= NOW() - INTERVAL 7 day
  AND nc.description not like '%%max pull test%%'
  AND (nc.description like '%%foreign%%' or nc.description like '%%fiber%%' or nc.description like '%%tape%%' or nc.description like '%%adhesive%%' or nc.description like '%%glove%%')
    """
    
    df = pd.read_sql(text(query), mos_con)
    mos_con.close()

    return df

def main(env):
    # Do not send an email on dev
    now=datetime.utcnow()
    pst_now = helper_functions.convert_from_utc_to_pst(now)
    logging.info("BMA123 Zone2/Zone3 Weekly FOD Summary" % pst_now)

    logging.info("BMA123 Zone2/Zone3 Weekly FOD Summary" % datetime.utcnow())

    df = get_bypassed_table()
    df = df[['LINE','EQUIPMENT','HOURS DOWN']]

    if len(df.index):
        message = """\
            <html>
            <head style="font-size:20px;"><strong>ACTA123 CURRENT BYPASSED EQUIPMENT</strong></head>
            <br></br>
            <body>
                {0}
            </body>
            </html>
            """.format(df.to_html(index=False,justify='left',bold_rows=True))
    else:
                message = """\
            <html>
            <head style="font-size:20px;"><strong>BMA123 Zone 2 Weekly FOD Summary</strong></head>
            <br></br>
            <body>
                Recap of this weeks module failures for FOD detected at Zone 3.
            </body>
            </html>
            """.format(df.to_html(index=False,justify='left',bold_rows=True))

    if env == 'prod':
        send_from = 'bma-pybot-passdown@tesla.com'
        send_to = ['BMA123-Z1@tesla.com',
                   'BMA123-Z2-PE@tesla.com',
                   'matolliver@tesla.com',
                   'apuliyaneth@tesla.com']
        subject = 'BMA123_Zone2 Weekly FOD Summary'
    else:
        send_from = 'bma-pybot-passdown-dev@tesla.com'
        send_to = ['matolliver@tesla.com']
        subject = 'BMA123_Zone2 Weekly FOD Summary (Dev)'

    try:
        helper_functions.send_mail(send_from,send_to,subject,message,'html')
    except Exception:
        logging.exception("failed to send exception email")
