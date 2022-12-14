from common import helper_functions

from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import pymsteams


def get_starved_table(db, start, end):
    pi_paths = [
        '[MC1_Zone1]_OEE_Reporting/TSMs/MC1_01000_01_Module Load_R01',
        '[MC2_10000_01]_OEE_Reporting/TSMs/MC2_10000_01_ST010_MDL10_Robot'
    ]
    po_paths = [
        '[MC1_Zone6]_OEE_Reporting/TSMs/MC1_30000_01_Robot26',
        '[MC2_28000_01]_OEE_Reporting/TSMs/MC2_28000_01_ST250_MDL10_Robot'
    ]

    seconds_between = (end - start).seconds

    pi_df = helper_functions.query_tsm_state(db, start, end, pi_paths, 'Starved', 1)
    po_df = helper_functions.query_tsm_state(db, start, end, po_paths, 'Starved', 2)

    pi1_starved = round(helper_functions.get_val(pi_df, 'MC1-', 'LINE', 'Duration') / seconds_between * 100, 1)
    pi2_starved = round(helper_functions.get_val(pi_df, 'MC2-', 'LINE', 'Duration') / seconds_between * 100, 1)

    po1_starved = round(helper_functions.get_val(po_df, 'MC1-', 'LINE', 'Duration') / seconds_between * 100, 1)
    po2_starved = round(helper_functions.get_val(po_df, 'MC2-', 'LINE', 'Duration') / seconds_between * 100, 1)

    html = f"""
        <tr>
            <td></td>
            <th style="text-align:center"><strong>MC1</strong></th>
            <th style="text-align:center"><strong>MC2</strong></th>
        </tr>
        <tr>
            <td style="text-align:left"><b>Pack-in MTRs</b></td>
            <td style="text-align:center">{pi1_starved}%</td>
            <td style="text-align:center">{pi2_starved}%</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>Pack-out MTRs</b></td>
            <td style="text-align:center">{po1_starved}%</td>
            <td style="text-align:center">{po2_starved}%</td>
        </tr>
        """
    return html


def main(env, eos=False):
    logging.info("Z4 start %s" % datetime.utcnow())

    lookback = 12 if eos else 1
    now = datetime.utcnow()
    now_sub1hr = now + timedelta(hours=-lookback)
    start = now_sub1hr.replace(minute=00, second=00, microsecond=00)
    end = start + timedelta(hours=lookback)

    MC1_FLOWSTEP = 'MC1-30000'
    MC2_FLOWSTEP = 'MC2-28000'

    flowsteps = [MC1_FLOWSTEP, MC2_FLOWSTEP]
    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    df_output = helper_functions.get_flowstep_outputs(mos_con, start, end, flowsteps)
    mc1_output = helper_functions.get_output_val(df_output, MC1_FLOWSTEP)
    mc2_output = helper_functions.get_output_val(df_output, MC2_FLOWSTEP)
    mic_total = mc1_output + mc2_output
    starve_table = get_starved_table(plc_con, start, end)  # pull starvation data

    # Setup teams output table
    title = 'Zone 4 Hourly Update'
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
                <td style="text-align:left">{mc2_output/4:.1f}</td>
            </tr>
            <tr>
                <td style="text-align:right"><strong>TOTAL</strong></td>
                <td style="text-align:left"><b>{mic_total/4:.1f}</b></td>
            </tr>
            </table>"""

    # Setup teams starvation table
    starved_html = "<table>" + "<caption>Starvation %</caption>" + starve_table + "</table>"

    # get webhook based on environment
    webhook_key = 'teams_webhook_Zone4_Updates' if env == 'prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']

    # start end of shift message
    teams_msg = pymsteams.connectorcard(webhook)
    title = 'Zone 4 EOS Report' if eos else 'Zone 4 Hourly Update'
    teams_msg.title(title)
    teams_msg.summary('summary')
    K8S_BLUE = '#3970e4'
    TESLA_RED = '#cc0000'
    msg_color = TESLA_RED if eos else K8S_BLUE
    teams_msg.color(msg_color)

    # create cards for each major html
    output_card = pymsteams.cardsection()
    output_card.text(html)
    teams_msg.addSection(output_card)
    # make a card with starvation data
    starved_card = pymsteams.cardsection()
    starved_card.text(starved_html)
    teams_msg.addSection(starved_card)
    # add a link to the confluence page
    teams_msg.addLinkButton("Questions?",
                            "https://confluence.teslamotors.com/display/PRODENG/Battery+Module+Hourly+Update")
    # SEND IT
    teams_msg.send()
