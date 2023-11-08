from common import helper_functions
from resources import outputz1
from resources import outputz2_123
from resources import outputz2_45
from resources import outputz2_8
from resources import outputz3
from resources import outputz4
from resources import NCM_bandolier_milan_output

import numpy as np
import pandas as pd
import pymsteams
from datetime import datetime
from datetime import timedelta
import logging
import traceback

def main(env):
    it_is_eos,it_is_24 = helper_functions.is_it_eos_or_24()
    if it_is_eos:
        logging.info('Running End of Shift Report')
        outputz1.main(env,eos=True)
        outputz2_123.main(env,eos=True)
        outputz2_45.main(env,eos=True)
        outputz2_8.main(env,eos=True)
        outputz3.main(env,eos=True)
        outputz4.main(env,eos=True)
        NCM_bandolier_milan_output.main(env,eos=True)
        eos_report(env)
        if it_is_24:
            eos_report(env,do_24=True)

def eos_report(env,do_24=False):
    logging.info('Start Battery End of Shift. 24_Hour Value: %s' % do_24)
    #define globals
    NORMAL_DIVISOR = 4
    CTA_DIVISOR = 28
    #define all flowsteps to be used
    DF_FLOWSTEP = pd.DataFrame({
                                    'LINE' : ['3BM1','3BM2','3BM3','3BM4','3BM5','3BM6','3BM8'],
                                    'CTA'  : ['3BM-20000','3BM-20000','3BM-20000','3BM4-25000','3BM5-25000','3BM6-25000','3BM8-25000'],
                                    'MAMC'  : ['3BM-29500','3BM-29500','3BM-29500','3BM4-34000','3BM5-34000','','3BM8-29500'],
                                    'MAMC_296'  : ['3BM-29600','3BM-29600','3BM-29600','','','',''],
                                    'C3A'  : ['3BM-40001','3BM-40001','3BM-40001','3BM4-45000','3BM5-45000','','3BM8-44000'],
                                    'ZONE3'  : ['3BM-57000','3BM-57000','3BM-57000','3BM-57000','3BM-57000','',''],
                                    'ZONE4'  : ['MC1-30000','MC2-28000','','','','','']
                                    })
    #get start and end of shift times
    now=datetime.utcnow()
    shift_end = now.replace(minute=00,second=00,microsecond=00)
    lookback=24 if do_24 else 12 #1 hr
    shift_start=shift_end+timedelta(hours=-lookback)

    #create mos connection
    mos_con = helper_functions.get_sql_conn('mos_rpt2')

    #make flowstep list
    all_flows_list = list(DF_FLOWSTEP['CTA']) + list(DF_FLOWSTEP['MAMC']) + list(DF_FLOWSTEP['MAMC_296']) + list(DF_FLOWSTEP['C3A']) + list(DF_FLOWSTEP['ZONE3']) + list(DF_FLOWSTEP['ZONE4'])

    #loop through each hour over the 12 hour shift
    df_output = helper_functions.get_flowstep_outputs(mos_con,shift_start,shift_end,all_flows_list)
    mos_con.close()

    #create lists for each zone output
    cta_outputs = []
    mamc_outputs = []
    mamc_296_outputs = []
    c3a_outputs = []
    z3_outputs = []
    z4_outputs = []
    #loop through lines and append outputs to each list
    for line in list(DF_FLOWSTEP['LINE']):
        cta_flowstep = DF_FLOWSTEP.query(f"LINE=='{line}'").iloc[0]['CTA']
        mamc_flowstep = DF_FLOWSTEP.query(f"LINE=='{line}'").iloc[0]['MAMC']
        mamc_296_flowstep = DF_FLOWSTEP.query(f"LINE=='{line}'").iloc[0]['MAMC_296']
        c3a_flowstep = DF_FLOWSTEP.query(f"LINE=='{line}'").iloc[0]['C3A']
        z3_flowstep = DF_FLOWSTEP.query(f"LINE=='{line}'").iloc[0]['ZONE3']

        cta_outputs.append(helper_functions.get_output_val(df_output,cta_flowstep,line))
        mamc_outputs.append(helper_functions.get_output_val(df_output,mamc_flowstep,line))
        mamc_296_outputs.append(helper_functions.get_output_val(df_output,mamc_296_flowstep,line))
        c3a_outputs.append(helper_functions.get_output_val(df_output,c3a_flowstep,line))
        z3_outputs.append(helper_functions.get_output_val(df_output,z3_flowstep,line))

        #special if statement for MC1/MC2
        if line in ['3BM1','3BM2']:
            z4_flowstep = DF_FLOWSTEP.query(f"LINE=='{line}'").iloc[0]['ZONE4']
            mc_line = line.replace('3BM','MC')
            z4_outputs.append(helper_functions.get_output_val(df_output,z4_flowstep,mc_line))

    cta123_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM1'").iloc[0]['CTA']
    cta4_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM4'").iloc[0]['CTA']
    cta5_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM5'").iloc[0]['CTA']
    cta6_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM6'").iloc[0]['CTA']
    cta8_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM8'").iloc[0]['CTA']

    mamc123_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM1'").iloc[0]['MAMC']
    mamc123_296_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM1'").iloc[0]['MAMC_296']
    mamc4_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM4'").iloc[0]['MAMC']
    mamc5_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM5'").iloc[0]['MAMC']
    mamc8_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM8'").iloc[0]['MAMC']

    c3a123_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM1'").iloc[0]['C3A']
    c3a4_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM4'").iloc[0]['C3A']
    c3a5_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM5'").iloc[0]['C3A']
    c3a8_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM8'").iloc[0]['C3A']

    z3_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM1'").iloc[0]['ZONE3']
    z4_mc1_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM1'").iloc[0]['ZONE4']
    z4_mc2_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM2'").iloc[0]['ZONE4']

    mamc_outputs = np.add(mamc_outputs,mamc_296_outputs)

    total_cta_ouput = helper_functions.get_output_val(df_output,cta123_flowstep) + helper_functions.get_output_val(df_output,cta4_flowstep) + helper_functions.get_output_val(df_output,cta5_flowstep) + helper_functions.get_output_val(df_output,cta6_flowstep) + helper_functions.get_output_val(df_output,cta8_flowstep)
    total_mamc_ouput = helper_functions.get_output_val(df_output,mamc123_flowstep) + helper_functions.get_output_val(df_output,mamc123_296_flowstep) + helper_functions.get_output_val(df_output,mamc4_flowstep) + helper_functions.get_output_val(df_output,mamc5_flowstep) + helper_functions.get_output_val(df_output,mamc8_flowstep)
    total_c3a_output = helper_functions.get_output_val(df_output,c3a123_flowstep) + helper_functions.get_output_val(df_output,c3a4_flowstep) + helper_functions.get_output_val(df_output,c3a5_flowstep) + helper_functions.get_output_val(df_output,c3a8_flowstep)
    total_z3_output = helper_functions.get_output_val(df_output,z3_flowstep)
    total_z4_output = helper_functions.get_output_val(df_output,z4_mc1_flowstep) + helper_functions.get_output_val(df_output,z4_mc2_flowstep) 

    #create bma header
    bma_header_html = f"""<tr>
            <th style="text-align:center"></th>
            <th style="text-align:center">BMA1</th>
            <th style="text-align:center">BMA2</th>
            <th style="text-align:center">BMA3</th>
            <th style="text-align:center">BMA4</th>
            <th style="text-align:center">BMA5</th>
            <th style="text-align:center">BMA6</th>
            <th style="text-align:center">BMA8</th>
            <th style="text-align:center">TOTAL</th>
            </tr>
            """

    #create cta output row
    cta_html = f"""<tr>
            <td style="text-align:center"><strong>ZONE1 CTA</strong></td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">{cta_outputs[1]/CTA_DIVISOR:.1f}</td>
            <td style="text-align:center">{cta_outputs[2]/CTA_DIVISOR:.1f}</td>
            <td style="text-align:center">{cta_outputs[3]/CTA_DIVISOR:.1f}</td>
            <td style="text-align:center">{cta_outputs[4]/CTA_DIVISOR:.1f}</td>
            <td style="text-align:center">{cta_outputs[5]/CTA_DIVISOR:.1f}</td>
            <td style="text-align:center">{cta_outputs[6]/CTA_DIVISOR:.1f}</td>
            <td style="text-align:center"><strong>{total_cta_ouput/CTA_DIVISOR:.1f}</strong></td>
            </tr>
            """
    #create mamc output row
    mamc_html = f"""<tr>
            <td style="text-align:center"><strong>ZONE2 MAMC</strong></td>
            <td style="text-align:center">{mamc_outputs[0]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{mamc_outputs[1]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{mamc_outputs[2]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{mamc_outputs[3]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{mamc_outputs[4]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">{mamc_outputs[6]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center"><strong>{(total_mamc_ouput)/NORMAL_DIVISOR:.1f}</strong></td>
            </tr>
            """
    #create c3a output row
    c3a_html = f"""<tr>
            <td style="text-align:center"><strong>ZONE2 C3A</strong></td>
            <td style="text-align:center">{c3a_outputs[0]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{c3a_outputs[1]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{c3a_outputs[2]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{c3a_outputs[3]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{c3a_outputs[4]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">{c3a_outputs[6]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center"><strong>{total_c3a_output/NORMAL_DIVISOR:.1f}</strong></td>
            </tr>
            """

    #create full bma html with the above htmls
    bma_html = '<table>' + bma_header_html + cta_html + mamc_html + c3a_html + '</table>'

    #create z3 html
    z3_html = f"""<table>
            <tr>
            <th style="text-align:center"></th>
            <th style="text-align:center">3BM1</th>
            <th style="text-align:center">3BM2</th>
            <th style="text-align:center">3BM3</th>
            <th style="text-align:center">3BM4</th>
            <th style="text-align:center">3BM5</th>
            <th style="text-align:center">TOTAL</th>
            </tr>
            <tr>
            <td style="text-align:center"><strong>ZONE3</strong></td>
            <td style="text-align:center">{z3_outputs[0]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{z3_outputs[1]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{z3_outputs[2]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{z3_outputs[3]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{z3_outputs[4]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center"><strong>{total_z3_output/NORMAL_DIVISOR:.1f}</strong></td>
            </tr>
            </table>
            """

    #create z4 html
    z4_html = f"""<table>
            <tr>
            <th style="text-align:center"></th>
            <th style="text-align:center">MC1</th>
            <th style="text-align:center">MC2</th>
            <th style="text-align:center">TOTAL</th>
            </tr>
            <tr>
            <td style="text-align:center"><strong>ZONE4</strong></td>
            <td style="text-align:center">{z4_outputs[0]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{z4_outputs[1]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center"><strong>{total_z4_output/NORMAL_DIVISOR:.1f}</strong></td>
            </tr>
            </table>
            """

    #get webhook based on environment
    webhook_key = 'teams_webhook_end_of_shift' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']

    #start end of shift message
    eos_msg = pymsteams.connectorcard(webhook)
    title = '24-Hour Report' if do_24 else 'End Of Shift Report'
    eos_msg.title(title)
    eos_msg.summary('summary')
    eos_msg.color('#cc0000')

    #create cards for each major html
    bma_card = pymsteams.cardsection()
    bma_card.text(bma_html)
    z3_card = pymsteams.cardsection()
    z3_card.text(z3_html)
    z4_card = pymsteams.cardsection()
    z4_card.text(z4_html)
    eos_msg.addSection(bma_card)
    eos_msg.addSection(z3_card)
    eos_msg.addSection(z4_card)
    #SEND IT
    try:
        eos_msg.send()
    except pymsteams.TeamsWebhookException:
        logging.warn("Webhook timed out, retry once")
        try:
            eos_msg.send()
        except pymsteams.TeamsWebhookException:
            logging.exception("Webhook timed out twice -- pass to next area")

    if do_24:
        # CTA 24hr Records
        webhook_key = 'teams_webhook_Zone1_Records' if env=='prod' else 'teams_webhook_DEV_Updates'
        webhook_json = helper_functions.get_pw_json(webhook_key)
        webhook = webhook_json['url']
        cta2 = cta_outputs[1]/CTA_DIVISOR
        cta3 = cta_outputs[2]/CTA_DIVISOR
        cta4 = cta_outputs[3]/CTA_DIVISOR
        cta5 = cta_outputs[4]/CTA_DIVISOR
        cta6 = cta_outputs[5]/CTA_DIVISOR
        cta8 = cta_outputs[6]/CTA_DIVISOR
        outputz1.cta_records(24,cta3,cta4,cta5,cta6,cta8,webhook)

        # C3A123 24hr records
        webhook_key = 'teams_webhook_Zone2_123_Records' if env=='prod' else 'teams_webhook_DEV_Updates'
        webhook_json = helper_functions.get_pw_json(webhook_key)
        webhook = webhook_json['url']
        c3a1 = c3a_outputs[0]/NORMAL_DIVISOR
        c3a2 = c3a_outputs[1]/NORMAL_DIVISOR
        c3a3 = c3a_outputs[2]/NORMAL_DIVISOR
        outputz2_123.ac3a_records(24,c3a1,c3a2,c3a3,webhook)

        # C3A8 24hr records
        c3a8 = c3a_outputs[5]/NORMAL_DIVISOR
        webhook_key = 'teams_webhook_BMA8_Records' if env=='prod' else 'teams_webhook_DEV_Updates'
        webhook_json = helper_functions.get_pw_json(webhook_key)
        webhook = webhook_json['url']
        outputz2_8.bma8_records(lookback,c3a8,webhook)
