import common.helper_functions as helper_functions

from datetime import datetime
from datetime import timedelta
from sqlalchemy import text

import logging
import pandas as pd
import numpy as np

def get_bypassed_table():
    tables = [44,46,41]
    count = []
    plc_con = helper_functions.get_sql_conn('plc_db')
    query = f"""
        SELECT DISTINCT
            (equipment_id),
            e.name AS 'LOCATION',
            e.module AS 'EQUIPMENT',
            state,
            ROUND(TIME_TO_SEC(TIMEDIFF(NOW(), MAX(start_time))) / 3600 ,1) AS 'HOURS DOWN',
            MAX(start_time) AS 'StartTimeUTC',
            NOW()
        FROM
            rno_eqtstatushistory_batterymodule.equipment_state_history sh
                JOIN
            rno_eqtstatushistory_batterymodule.equipment e ON e.id = sh.equipment_id
        WHERE
            equipment_id IN (SELECT 
                    id
                FROM
                    rno_eqtstatushistory_batterymodule.equipment e
                WHERE
                    e.source_tagpath LIKE '%bypass%')
                AND sh.state = 1
                AND ISNULL(sh.end_time)
        GROUP BY 1
        ORDER BY 2
    """
    
    df = pd.read_sql(text(query), plc_con)
    plc_con.close()

    return df

def main(env):
    # Do not send an email on dev
    now=datetime.utcnow()
    pst_now = helper_functions.convert_from_utc_to_pst(now)
    logging.info("CTA123 Bypass Eqt Email Main Triggered %s PST" % pst_now)

    if env == 'prod':

        logging.info("CTA123 Bypass Eqt Email In Prod %s UTC" % datetime.utcnow())

        df = get_bypassed_table()
        df = df[['LOCATION','EQUIPMENT','HOURS DOWN']]

        message = """\
            <html>
            <head style="font-size:20px;"><strong>ACTA123 CURRENT BYPASSED EQUIPMENT</strong></head>
            <br></br>
            <body>
                {0}
            </body>
            </html>
            """.format(df.to_html(index=False,justify='left',bold_rows=True))

        # html = df.to_html(index=False,border=0,justify='left',bold_rows=True)

        send_from = 'bma-pybot-passdown@tesla.com'
        send_to = ['BMA123-Z1@tesla.com',
                   'mberlied@tesla.com',
                   'tikim@tesla.com']
        subject = 'ACTA123 Bypassed Equipment Report'

        try:
            helper_functions.send_mail(send_from,send_to,subject,message,'html')
        except Exception:
            logging.exception("failed to send exception email")
    else:
        pass