import logging
from datetime import datetime, timedelta

import common.helper_functions as helper_functions
import numpy as np
import pandas as pd
from sqlalchemy import text


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
                    e.source_tagpath LIKE '%bypass%'
                    and e.name NOT LIKE '3BM1%'
                    and e.name NOT LIKE '3BM2%')
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

    logging.info("CTA123 Bypass Eqt Email In Prod %s UTC" % datetime.utcnow())

    df = get_bypassed_table()
    df = df[['LOCATION','EQUIPMENT','HOURS DOWN']]

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
            <head style="font-size:20px;"><strong>ACTA123 CURRENT BYPASSED EQUIPMENT</strong></head>
            <br></br>
            <body>
                There are no bypasses active in ACTA123
            </body>
            </html>
            """.format(df.to_html(index=False,justify='left',bold_rows=True))

    if env == 'prod':
        send_from = 'bma-pybot-passdown@tesla.com'
        send_to = ['BMA123-Z1@tesla.com',
                   'BMA123-Z2-PE@tesla.com',
                   'tikim@tesla.com']
        subject = 'ACTA123 Bypassed Equipment Report'
    else:
        send_from = 'bma-pybot-passdown-dev@tesla.com'
        send_to = ['mberlied@tesla.com']
        subject = 'ACTA123 Bypassed Equipment Report (Dev)'

    try:
        helper_functions.send_mail(send_from,send_to,subject,message,'html')
    except Exception:
        logging.exception("failed to send exception email")
