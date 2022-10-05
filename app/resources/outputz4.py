from common import helper_functions

from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import pymsteams

def main(env,eos=False):
    logging.info("Z4 start %s" % datetime.utcnow())

    lookback=12 if eos else 1
    now=datetime.utcnow() 
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    MC1_FLOWSTEP = 'MC1-30000'
    MC2_FLOWSTEP = 'MC2-28000'

    flowsteps = [MC1_FLOWSTEP, MC2_FLOWSTEP]
    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)
    mc1_output=helper_functions.get_output_val(df_output,'MC1',MC1_FLOWSTEP)
    mc2_output=helper_functions.get_output_val(df_output,'MC2',MC2_FLOWSTEP)
    mic_total = mc1_output + mc2_output

    # Setup teams output table
    title='Zone 4 Hourly Update'
    html = f"""<table>
            <tr>
                <th style="text-align:right">LINE</th>
                <th style="text-align:center">UPH</th>
            </tr>
            <tr>
                <td style="text-align:right"><strong>MC1</strong></td>
                <td <td style="text-align:left">{mc1_output/4:.1f}</td>
            </tr>
            <tr>
                <td style="text-align:right"><strong>MC2</strong></td>
                <td style="text-align:left">{mc1_output/4:.1f}</td>
            </tr>
            <tr>
                <td style="text-align:right"><strong>TOTAL</strong></td>
                <td style="text-align:left"><b>{mic_total/4:.1f}</b></td>
            </tr>
            </table>"""


    #get webhook based on environment
    webhook_key = 'teams_webhook_Zone4_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']

    #start end of shift message
    teams_msg = pymsteams.connectorcard(webhook)
    title = 'Zone 4 EOS Report' if eos else 'Zone 4 Hourly Update'
    teams_msg.title(title)
    teams_msg.summary('summary')
    K8S_BLUE = '#3970e4'
    TESLA_RED = '#cc0000'
    msg_color = TESLA_RED if eos else K8S_BLUE
    teams_msg.color(msg_color)
    
    #create cards for each major html
    output_card = pymsteams.cardsection()
    output_card.text(html)
    teams_msg.addSection(output_card)
    #SEND IT
    teams_msg.send()