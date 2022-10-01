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

def output123(env,eos=False):
    eos=True
    #define start and end time for the hour
    lookback=12 if eos else 1
    now=datetime.utcnow()
    logging.info("Output123 start %s" % datetime.utcnow())
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    print(str(start))
    print(str(end))

    #define globals
    NORMAL_DIVISOR = 4
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
    if lookback>1:
        df_output = pd.DataFrame({})
        mmamc_output = 0
        while start < end:
            start_next = start + timedelta(minutes=60)
            df_output_sub = helper_functions.get_flowstep_outputs(mos_con,start,start_next,flowsteps)
            mmamc_output += get_mmamc_output(mos_con,start,start_next)
            df_output = pd.concat([df_output,df_output_sub],axis=0)
            start += timedelta(minutes=60)
    else:
        df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)
        mmamc_output = get_mmamc_output(mos_con,start,end)

    mos_con.close()

    cta_outputs = []
    mamc_outputs = []
    c3a_outputs = []
    cta1_outputs = []
    cta2_outputs = []
    cta3_outputs = []
    for line in LINES:
        cta_outputs.append(helper_functions.get_output_val(df_output,line,CTA_FLOWSTEP))
        mamc_outputs.append(helper_functions.get_output_val(df_output,line,MAMC_FLOWSTEP))
        c3a_outputs.append(helper_functions.get_output_val(df_output,line,C3A_FLOWSTEP))

    for lane in range(1,5):
        lane_num = str(lane).zfill(2)
        cta1_outputs.append(helper_functions.get_output_val(df_output,'3BM1',CTA_FLOWSTEP,actor=f"3BM1-20000-{lane_num}"))
        cta2_outputs.append(helper_functions.get_output_val(df_output,'3BM2',CTA_FLOWSTEP,actor=f"3BM2-20000-{lane_num}"))
        cta3_outputs.append(helper_functions.get_output_val(df_output,'3BM3',CTA_FLOWSTEP,actor=f"3BM3-20000-{lane_num}"))

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
            <td style="text-align:center">{cta_outputs[0]/CTA_DIVISOR:.1f}</td>
            <td style="text-align:center">{cta_outputs[1]/CTA_DIVISOR:.1f}</td>
            <td style="text-align:center">{cta_outputs[2]/CTA_DIVISOR:.1f}</td>
            <td style="text-align:center">----</td>
            <td style="text-align:center"><strong>{sum(cta_outputs)/CTA_DIVISOR:.1f}</strong></td>
            </tr>
    """
    #create mamc output row
    mamc_output_html = f"""<tr>
            <td style="text-align:center"><strong>MAMC</strong></td>
            <td style="text-align:center">{mamc_outputs[0]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{mamc_outputs[1]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{mamc_outputs[2]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{mmamc_output/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center"><strong>{(sum(mamc_outputs)+mmamc_output)/NORMAL_DIVISOR:.1f}</strong></td>
            </tr>
    """
    #create c3a output row
    c3a_output_html = f"""<tr>
            <td style="text-align:center"><strong>C3A</strong></td>
            <td style="text-align:center">{c3a_outputs[0]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{c3a_outputs[1]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{c3a_outputs[2]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">----</td>
            <td style="text-align:center"><strong>{sum(c3a_outputs)/NORMAL_DIVISOR:.1f}</strong></td>
            </tr>
    """

    #create full bma html with the above htmls
    bma_html = '<table>' + bma_header_html + cta_output_html + mamc_output_html + c3a_output_html + '</table>'

    #create cta header
    cta_header_html = """<tr>
                        <th style="text-align:center"></th>
                        <th style="text-align:center">Lane1</th>
                        <th style="text-align:center">Lane2</th>
                        <th style="text-align:center">Lane3</th>
                        <th style="text-align:center">Lane4</th>
                        </tr>
                """
    #create cta header
    cta_header_html = """<tr>
                        <th style="text-align:center"></th>
                        <th style="text-align:center">Lane1</th>
                        <th style="text-align:center">Lane2</th>
                        <th style="text-align:center">Lane3</th>
                        <th style="text-align:center">Lane4</th>
                        </tr>
                    """
    CTA_LANE_GOAL = 1.4
    cta1_html = """
                <tr>
                <td style="text-align:left"><strong>CTA1</strong></td>
                """
    cta2_html = """
                <tr>
                <td style="text-align:left"><strong>CTA2</strong></td>
                """
    cta3_html = """
                <tr>
                <td style="text-align:left"><strong>CTA3</strong></td>
                """
    for i,val in enumerate(cta1_outputs):
        color_str = "color:red;" if val/CTA_DIVISOR < CTA_LANE_GOAL else "font-weight:bold;"
        cta1_html += f"""
                    <td style="text-align:center;{color_str}">{val/CTA_DIVISOR:.1f}</td>
                    """

        color_str = "color:red;" if cta2_outputs[i]/CTA_DIVISOR < CTA_LANE_GOAL else "font-weight:bold;"
        cta2_html += f"""
                    <td style="text-align:center;{color_str}">{cta2_outputs[i]/CTA_DIVISOR:.1f}</td>
                    """

        color_str = "color:red;" if cta3_outputs[i]/CTA_DIVISOR < CTA_LANE_GOAL else "font-weight:bold;"
        cta3_html += f"""
                    <td style="text-align:center;{color_str}">{cta3_outputs[i]/CTA_DIVISOR:.1f}</td>
                    """

    cta1_html += "</tr>"
    cta2_html += "</tr>"
    cta3_html += "</tr>"

    #create full bma html with the above htmls
    cta_html = '<table>' + cta_header_html + cta1_html + cta2_html + cta3_html + '</table>'

    #get webhook based on environment
    webhook_key = 'teams_webhook_BMA123_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']

    #start end of shift message
    hourly_msg = pymsteams.connectorcard(webhook)
    hourly_msg.title('BMA123 Hourly Update')
    hourly_msg.summary('summary')
    hourly_msg.color('#3970e4')
    
    #create cards for each major html
    bma_card = pymsteams.cardsection()
    bma_card.text(bma_html)
    hourly_msg.addSection(bma_card)

    cta_card = pymsteams.cardsection()
    cta_card.text(cta_html)
    hourly_msg.addSection(cta_card)
    #SEND IT
    hourly_msg.send()