from common import helper_functions

from datetime import datetime
from datetime import timedelta
import logging
import numpy as np
import pandas as pd
import pymsteams
import traceback

def main(env,eos=False):
    try:
        #define start and end time for the hour
        lookback=12 if eos else 1
        now=datetime.utcnow()
        logging.info("Output8 start %s" % datetime.utcnow())
        now_sub1hr=now+timedelta(hours=-lookback)
        start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
        end=start+timedelta(hours=lookback)

        logging.info(str(start))
        logging.info(str(end))

        #define globals
        NORMAL_DIVISOR = 4
        MAMC_FLOWSTEP = 'MBM-25000'
        MAMC_LINE = 'MMAM'
        C3A_FLOWSTEP = '3BM8-44000'
        C3A_LINE = '3BM8'

        #create flowstep list
        flowsteps = [MAMC_FLOWSTEP,C3A_FLOWSTEP]
        #create mos connection
        mos_con = helper_functions.get_sql_conn('mos_rpt2')
        #get output for flowsteps
        df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)

        mos_con.close()

        mamc_outputs = helper_functions.get_output_val(df_output, MAMC_FLOWSTEP,MAMC_LINE)
        c3a_outputs = helper_functions.get_output_val(df_output,C3A_FLOWSTEP,C3A_LINE)

        #create bma header
        bma_header_html = f"""<tr>
                <th style="text-align:center"></th>
                <th style="text-align:center">BMA8</th>
                </tr>
        """
        #create mamc output row
        mamc_output_html = f"""<tr>
                <td style="text-align:center"><strong>MAMC</strong></td>
                <td style="text-align:center">{mamc_outputs/NORMAL_DIVISOR:.2f}</td>
                </tr>
        """
        #create c3a output row
        c3a_output_html = f"""<tr>
                <td style="text-align:center"><strong>C3A</strong></td>
                <td style="text-align:center">{c3a_outputs/NORMAL_DIVISOR:.2f}</td>
                </tr>
        """

        #create full bma html with the above htmls
        output_html = '<table>' + bma_header_html + mamc_output_html + c3a_output_html + '</table>'

        #get webhook based on environment
        webhook_key = 'teams_webhook_BMA8_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
        webhook_json = helper_functions.get_pw_json(webhook_key)
        webhook = webhook_json['url']

        #start end of shift message
        teams_msg = pymsteams.connectorcard(webhook)
        title = 'BMA8 EOS Report' if eos else 'BMA8 Hourly Update'
        teams_msg.title(title)
        teams_msg.summary('summary')
        K8S_BLUE = '#3970e4'
        TESLA_RED = '#cc0000'
        msg_color = TESLA_RED if eos else K8S_BLUE
        teams_msg.color(msg_color)
        
        #create cards for each major html
        output_card = pymsteams.cardsection()
        output_card.text(output_html)
        teams_msg.addSection(output_card)

        teams_msg.addLinkButton("Questions?", "https://confluence.teslamotors.com/display/PRODENG/Battery+Module+Hourly+Update")
        #SEND IT
        teams_msg.send()
    except Exception:
        logging.error(traceback.print_exc())