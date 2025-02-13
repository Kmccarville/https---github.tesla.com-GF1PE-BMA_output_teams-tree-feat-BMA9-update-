import logging
import multiprocessing as mp
import time
import traceback
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pymsteams
from common import helper_functions
from common.constants import Z1_DIVISOR, Z2_DIVISOR, Z3_DIVISOR, Z4_DIVISOR
from resources import (NCM_bandolier_milan_output, NCM_module_output, outputz1,
                       outputz2_8, outputz2_45, outputz2_123, outputz3,
                       outputz4)


def main(env,local_run=False):
    it_is_eos,it_is_24 = helper_functions.is_it_eos_or_24()
    if it_is_eos or local_run:
        if env == "dev":             
            start_time = time.perf_counter()
        logging.info('Running End of Shift Report')
        
        calls = [
           outputz1.main,
           outputz2_123.main,
           outputz2_45.main,
           outputz2_8.main,
           outputz3.main,
           outputz4.main,
           NCM_bandolier_milan_output.main,
           NCM_module_output.main
        ]
        
        procs = []
        for call in calls:
            proc = mp.Process(target=call, args=(env, True,))
            procs.append(proc)
            proc.start()
        for proc in procs:
            proc.join()

        if env == "dev":
            finish_time = time.perf_counter()
            logging.info("EOS Report Time: " + str(finish_time - start_time) + " Seconds")
   
        eos_report(env)
        if it_is_24 or local_run:
            eos_report(env,do_24=True)

def eos_report(env,do_24=False):
    logging.info('Start Battery End of Shift. 24_Hour Value: %s' % do_24)
    #define all flowsteps to be used
    DF_FLOWSTEP = pd.DataFrame({
                                    'LINE' : ['3BM1','3BM2','3BM3','3BM4','3BM5','3BM6','3BM7','3BM8','GFNV'],
                                    'CTA'  : ['','','','3BM4-25000','3BM5-25000','3BM6-25000','3BM7-25000','3BM8-25000','GFNV-BT1-3BM-25000'],
                                    'MAMC'  : ['3BM-29500','3BM-29500','3BM-29500','3BM4-34000','3BM5-34000','','','3BM8-29500',''],
                                    'MAMC_296'  : ['3BM-29600','3BM-29600','3BM-29600','','','','','',''],
                                    'C3A'  : ['3BM-40001','3BM-40001','3BM-40001','3BM4-45000','3BM5-45000','','','3BM8-44000',''],
                                    'ZONE3'  : ['3BM-57000','3BM-57000','3BM-57000','3BM-57000','3BM-57000','','','',''],
                                    'ZONE4'  : ['MC1-30000','MC2-28000','','','','','','','']
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

    cta4_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM4'").iloc[0]['CTA']
    cta5_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM5'").iloc[0]['CTA']
    cta6_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM6'").iloc[0]['CTA']
    cta7_flowstep = DF_FLOWSTEP.query(f"LINE=='3BM7'").iloc[0]['CTA']
    cta9_flowstep = DF_FLOWSTEP.query(f"LINE=='GFNV'").iloc[0]['CTA']

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

    total_cta_ouput = helper_functions.get_output_val(df_output,cta4_flowstep) + helper_functions.get_output_val(df_output,cta5_flowstep) + helper_functions.get_output_val(df_output,cta6_flowstep) + helper_functions.get_output_val(df_output,cta7_flowstep) + helper_functions.get_output_val(df_output,cta9_flowstep)
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
            <th style="text-align:center">BMA7</th>
            <th style="text-align:center">BMA8</th>
            <th style="text-align:center">BMA9</th>
            <th style="text-align:center">TOTAL</th>
            </tr>
            """

    #create cta output row
    cta_html = f"""<tr>
            <td style="text-align:center"><strong>ZONE1 CTA</strong></td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">{cta_outputs[3]/Z1_DIVISOR:.1f}</td>
            <td style="text-align:center">{cta_outputs[4]/Z1_DIVISOR:.1f}</td>
            <td style="text-align:center">{cta_outputs[5]/Z1_DIVISOR:.1f}</td>
            <td style="text-align:center">{cta_outputs[6]/Z1_DIVISOR:.1f}</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">{cta_outputs[8]/Z1_DIVISOR:.1f}</td>
            <td style="text-align:center"><strong>{total_cta_ouput/Z1_DIVISOR:.1f}</strong></td>
            </tr>
            """
    #create mamc output row
    mamc_html = f"""<tr>
            <td style="text-align:center"><strong>ZONE2 MAMC</strong></td>
            <td style="text-align:center">{mamc_outputs[0]/Z2_DIVISOR:.1f}</td>
            <td style="text-align:center">{mamc_outputs[1]/Z2_DIVISOR:.1f}</td>
            <td style="text-align:center">{mamc_outputs[2]/Z2_DIVISOR:.1f}</td>
            <td style="text-align:center">{mamc_outputs[3]/Z2_DIVISOR:.1f}</td>
            <td style="text-align:center">{mamc_outputs[4]/Z2_DIVISOR:.1f}</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">{mamc_outputs[7]/Z2_DIVISOR:.1f}</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center"><strong>{(total_mamc_ouput)/Z2_DIVISOR:.1f}</strong></td>
            </tr>
            """
    #create c3a output row
    c3a_html = f"""<tr>
            <td style="text-align:center"><strong>ZONE2 C3A</strong></td>
            <td style="text-align:center">{c3a_outputs[0]/Z3_DIVISOR:.1f}</td>
            <td style="text-align:center">{c3a_outputs[1]/Z3_DIVISOR:.1f}</td>
            <td style="text-align:center">{c3a_outputs[2]/Z3_DIVISOR:.1f}</td>
            <td style="text-align:center">{c3a_outputs[3]/Z3_DIVISOR:.1f}</td>
            <td style="text-align:center">{c3a_outputs[4]/Z3_DIVISOR:.1f}</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">{c3a_outputs[7]/Z3_DIVISOR:.1f}</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center"><strong>{total_c3a_output/Z3_DIVISOR:.1f}</strong></td>
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
            <td style="text-align:center">{z3_outputs[0]/Z3_DIVISOR:.1f}</td>
            <td style="text-align:center">{z3_outputs[1]/Z3_DIVISOR:.1f}</td>
            <td style="text-align:center">{z3_outputs[2]/Z3_DIVISOR:.1f}</td>
            <td style="text-align:center">{z3_outputs[3]/Z3_DIVISOR:.1f}</td>
            <td style="text-align:center">{z3_outputs[4]/Z3_DIVISOR:.1f}</td>
            <td style="text-align:center"><strong>{total_z3_output/Z3_DIVISOR:.1f}</strong></td>
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
            <td style="text-align:center">{z4_outputs[0]/Z4_DIVISOR:.1f}</td>
            <td style="text-align:center">{z4_outputs[1]/Z4_DIVISOR:.1f}</td>
            <td style="text-align:center"><strong>{total_z4_output/Z4_DIVISOR:.1f}</strong></td>
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
        cta4 = cta_outputs[3]/Z1_DIVISOR
        cta5 = cta_outputs[4]/Z1_DIVISOR
        cta6 = cta_outputs[5]/Z1_DIVISOR
        cta7 = cta_outputs[6]/Z1_DIVISOR
        cta9 = cta_outputs[8]/Z1_DIVISOR
        outputz1.cta_records(24,cta4,cta5,cta6,cta7,cta9,webhook)

        # C3A123 24hr records
        webhook_key = 'teams_webhook_Zone2_123_Records' if env=='prod' else 'teams_webhook_DEV_Updates'
        webhook_json = helper_functions.get_pw_json(webhook_key)
        webhook = webhook_json['url']
        c3a1 = c3a_outputs[0]/Z2_DIVISOR
        c3a2 = c3a_outputs[1]/Z2_DIVISOR
        c3a3 = c3a_outputs[2]/Z2_DIVISOR
        outputz2_123.ac3a_records(24,c3a1,c3a2,c3a3,webhook)

        # C3A8 24hr records
        c3a8 = c3a_outputs[5]/Z2_DIVISOR
        webhook_key = 'teams_webhook_BMA8_Records' if env=='prod' else 'teams_webhook_DEV_Updates'
        webhook_json = helper_functions.get_pw_json(webhook_key)
        webhook = webhook_json['url']
        outputz2_8.bma8_records(lookback,c3a8,webhook)