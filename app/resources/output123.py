from common import helper_functions

from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import pymsteams

def get_mmamc_output(db,start,end):
    query = """
            SELECT count(distinct tp.thingid) as OUTPUT
            FROM thingpath tp
            WHERE
                tp.flowstepname = 'MBM-25000'
                AND tp.exitcompletioncode = 'PASS'
                AND tp.completed BETWEEN '{start}' AND '{end}'    
            """
    df = pd.read_sql(query,db)
    output = df.iloc[0]['OUTPUT'] if len(df) else 0
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
    mos_con = helper_functions.get_sql_conn('mosrpt1')
    #get output for flowsteps
    df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)
    mmamc_output = get_mmamc_output(df_output,start,end)
    mos_con.close()

    cta_outputs = []
    mamc_outputs = []
    c3a_outputs = []
    for line in LINES:
        cta_outputs.append(helper_functions.get_output_val(df_output,line,CTA_FLOWSTEP,divisor=CTA_DIVISOR))
        mamc_outputs.append(helper_functions.get_output_val(df_output,line,MAMC_FLOWSTEP))
        c3a_outputs.append(helper_functions.get_output_val(df_output,line,C3A_FLOWSTEP))

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
    cta_html = f"""<tr>
            <td style="text-align:center"><strong>CTA</strong></td>
            <td style="text-align:center">{cta_outputs[0]}</td>
            <td style="text-align:center">{cta_outputs[1]}</td>
            <td style="text-align:center">{cta_outputs[2]}</td>
            <td style="text-align:center">----</td>
            <td style="text-align:center"><strong>{sum(cta_outputs)}</strong></td>
            </tr>
    """
    #create mamc output row
    mamc_html = f"""<tr>
            <td style="text-align:center"><strong>MAMC</strong></td>
            <td style="text-align:center">{mamc_outputs[0]}</td>
            <td style="text-align:center">{mamc_outputs[1]}</td>
            <td style="text-align:center">{mamc_outputs[2]}</td>
            <td style="text-align:center">{mmamc_output}</td>
            <td style="text-align:center"><strong>{sum(mamc_outputs)+mmamc_output}</strong></td>
            </tr>
    """
    #create c3a output row
    c3a_html = f"""<tr>
            <td style="text-align:center"><strong>C3A</strong></td>
            <td style="text-align:center">{c3a_outputs[0]}</td>
            <td style="text-align:center">{c3a_outputs[1]}</td>
            <td style="text-align:center">{c3a_outputs[2]}</td>
            <td style="text-align:center">----</td>
            <td style="text-align:center"><strong>{sum(c3a_outputs)}</strong></td>
            </tr>
    """

    #create full bma html with the above htmls
    bma_html = '<table>' + bma_header_html + cta_html + mamc_html + c3a_html + '</table>'

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
    #SEND IT
    eos_msg.send()