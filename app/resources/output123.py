from common import helper_functions

from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import pymsteams

def get_mmamc_output(db,start,end):
    query = f"""
            SELECT count(distinct tp.thingid) as OUTPUT
            FROM sparq.thingpath tp
            WHERE
                tp.flowstepname = 'MBM-25000'
                AND tp.exitcompletioncode = 'PASS'
                AND tp.completed BETWEEN '{start}' AND '{end}'    
            """
    df = pd.read_sql(query,db)
    output = round(df.iloc[0]['OUTPUT']/4,1) if len(df) else 0 
    return output

def output123(env):
    #define start and end time for the hour
    lookback=1 #1 hr
    now=datetime.utcnow()
    logging.info("Output123 start %s" % datetime.utcnow())
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    #define globals
    CTA_DIVISOR = 28
    CTA_FLOWSTEP = '3BM-20000'
    MAMC_FLOWSTEP= '3BM-29500'
    C3A_FLOWSTEP = '3BM-40001'
    LINES = ['3BM1','3BM2','3BM3']

    #create flowstep list
    flowsteps = [CTA_FLOWSTEP,MAMC_FLOWSTEP,C3A_FLOWSTEP]
    #create mos connection
    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    #get output for flowsteps
    df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)
    mmamc_output = get_mmamc_output(mos_con,start,end)
    mos_con.close()

    cta_outputs = []
    mamc_outputs = []
    c3a_outputs = []
    cta_l1_outputs = []
    cta_l2_outputs = []
    cta_l3_outputs = []
    cta_l4_outputs = []
    for line in LINES:
        cta_outputs.append(helper_functions.get_output_val(df_output,line,CTA_FLOWSTEP,divisor=CTA_DIVISOR))
        mamc_outputs.append(helper_functions.get_output_val(df_output,line,MAMC_FLOWSTEP))
        c3a_outputs.append(helper_functions.get_output_val(df_output,line,C3A_FLOWSTEP))
        cta_l1_outputs.append(helper_functions.get_output_val(df_output,line,CTA_FLOWSTEP,actor=f'{line}-20000-01',divisor=CTA_DIVISOR))
        cta_l2_outputs.append(helper_functions.get_output_val(df_output,line,CTA_FLOWSTEP,actor=f'{line}-20000-02',divisor=CTA_DIVISOR))
        cta_l3_outputs.append(helper_functions.get_output_val(df_output,line,CTA_FLOWSTEP,actor=f'{line}-20000-03',divisor=CTA_DIVISOR))
        cta_l4_outputs.append(helper_functions.get_output_val(df_output,line,CTA_FLOWSTEP,actor=f'{line}-20000-04',divisor=CTA_DIVISOR))
    #create bma header
    bma_header_html = """<tr>
            <th style="text-align:center"></th>
            <th style="text-align:center">BMA1</th>
            <th style="text-align:center">BMA2</th>
            <th style="text-align:center">BMA3</th>
            <th style="text-align:center">MMAMC</th>
            <th style="text-align:center">TOTAL</th>
            </tr>
    """

    #create cta output row
    cta_output_html = f"""<tr>
            <td style="text-align:center"><strong>CTA</strong></td>
            <td style="text-align:center">{cta_outputs[0]}</td>
            <td style="text-align:center">{cta_outputs[1]}</td>
            <td style="text-align:center">{cta_outputs[2]}</td>
            <td style="text-align:center">----</td>
            <td style="text-align:center"><strong>{sum(cta_outputs)}</strong></td>
            </tr>
    """
    #create mamc output row
    mamc_output_html = f"""<tr>
            <td style="text-align:center"><strong>MAMC</strong></td>
            <td style="text-align:center">{mamc_outputs[0]}</td>
            <td style="text-align:center">{mamc_outputs[1]}</td>
            <td style="text-align:center">{mamc_outputs[2]}</td>
            <td style="text-align:center">{mmamc_output}</td>
            <td style="text-align:center"><strong>{sum(mamc_outputs)+mmamc_output}</strong></td>
            </tr>
    """
    #create c3a output row
    c3a_output_html = f"""<tr>
            <td style="text-align:center"><strong>C3A</strong></td>
            <td style="text-align:center">{c3a_outputs[0]}</td>
            <td style="text-align:center">{c3a_outputs[1]}</td>
            <td style="text-align:center">{c3a_outputs[2]}</td>
            <td style="text-align:center">----</td>
            <td style="text-align:center"><strong>{sum(c3a_outputs)}</strong></td>
            </tr>
    """

    #create full bma html with the above htmls
    bma_html = '<table>' + bma_header_html + cta_output_html + mamc_output_html + c3a_output_html + '</table>'

    #create cta header
    cta_header_html = """<tr>
                        <th style="text-align:center"></th>
                        <th style="text-align:center">L1</th>
                        <th style="text-align:center">L2</th>
                        <th style="text-align:center">L3</th>
                        <th style="text-align:center">L4</th>
                        </tr>
                """
    #create cta123 output rows
    cta_l1_html = f"""<tr>
                        <td style="text-align:center"><strong>CTA1</strong></td>
                        <td style="text-align:center">{cta_l1_outputs[0]}</td>
                        <td style="text-align:center">{cta_l1_outputs[1]}</td>
                        <td style="text-align:center">{cta_l1_outputs[2]}</td>
                        <td style="text-align:center">{cta_l1_outputs[3]}</td>
                        </tr>
                """
    #create cta123 output rows
    cta_l2_html = f"""<tr>
                        <td style="text-align:center"><strong>CTA2</strong></td>
                        <td style="text-align:center">{cta_l2_outputs[0]}</td>
                        <td style="text-align:center">{cta_l2_outputs[1]}</td>
                        <td style="text-align:center">{cta_l2_outputs[2]}</td>
                        <td style="text-align:center">{cta_l2_outputs[3]}</td>
                        </tr
                """
    #create cta123 output rows
    cta_l3_html = f"""<tr>
                        <td style="text-align:center"><strong>CTA3</strong></td>
                        <td style="text-align:center">{cta_l3_outputs[0]}</td>
                        <td style="text-align:center">{cta_l3_outputs[1]}</td>
                        <td style="text-align:center">{cta_l3_outputs[2]}</td>
                        <td style="text-align:center">{cta_l3_outputs[3]}</td>
                        </tr>
                """

    #create full bma html with the above htmls
    cta_html = '<table>' + cta_header_html + cta_l1_html + cta_l2_html + cta_l3_html + '</table>'

    #get webhook based on environment
    webhook_key = 'teams_webhook_BMA123_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']

    #start end of shift message
    eos_msg = pymsteams.connectorcard(webhook)
    eos_msg.title('BMA123 Hourly Update')
    eos_msg.summary('summary')
    eos_msg.color('#3970e4')
    
    #create cards for each major html
    bma_card = pymsteams.cardsection()
    bma_card.text(bma_html)
    eos_msg.addSection(bma_card)

    cta_card = pymsteams.cardsection()
    cta_card.text(cta_html)
    eos_msg.addSection(cta_card)
    #SEND IT
    eos_msg.send()