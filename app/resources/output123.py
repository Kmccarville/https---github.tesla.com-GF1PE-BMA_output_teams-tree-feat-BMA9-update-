from common import helper_functions

from datetime import datetime
from datetime import timedelta
import logging
import numpy as np
import pandas as pd
import pymsteams

def get_mmamc_output(db,start,end):
    df = pd.DataFrame({})
    while start < end:
        start_next = start + timedelta(minutes=60)
        query = f"""
            SELECT count(distinct tp.thingid) as OUTPUT
            FROM sparq.thingpath tp
            WHERE
                tp.flowstepname = 'MBM-25000'
                AND tp.exitcompletioncode = 'PASS'
                AND tp.completed BETWEEN '{start}' AND '{start_next}'    
            """
        df_sub = pd.read_sql(query,db)
        df = pd.concat([df,df_sub],axis=0)
        start += timedelta(minutes=60)

    output = df['OUTPUT'].sum() if len(df) else 0 
    return output

def get_mamc_yield_table(start,end):
    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    df = pd.DataFrame({})
    while start < end:
        start_next = start + timedelta(minutes=60)
        query = f"""
                SELECT
                left(a.name,4) as LINE,
                ((COUNT(DISTINCT tp.thingid) - COUNT(DISTINCT nc.thingid)) / COUNT(DISTINCT tp.thingid)* 100) AS YIELD
                FROM
                    sparq.thingpath tp
                    JOIN sparq.actor a ON a.id = tp.actorcreatedby
                    LEFT JOIN
                    sparq.nc ON nc.thingid = tp.thingid
                WHERE
                    tp.flowstepname IN ('3BM-29500')
                    AND tp.completed BETWEEN '{start}' AND '{start_next}'
                GROUP BY 1
                """
        df_sub = pd.read_sql(query,mos_con)
        df = pd.concat([df,df_sub],axis=0)
        start += timedelta(minutes=60)

    mos_con.close()

    bma1_yield = round(helper_functions.get_val(df,'3BM1','LINE','YIELD'),1)
    bma2_yield = round(helper_functions.get_val(df,'3BM2','LINE','YIELD'),1)
    bma3_yield = round(helper_functions.get_val(df,'3BM3','LINE','YIELD'),1)

    html=f"""
    <tr>
        <td style="text-align:center"><b>MAMC Yield</b></td>
        <td style="text-align:center">{bma1_yield}%</td>
        <td style="text-align:center">{bma2_yield}%</td>
        <td style="text-align:center">{bma3_yield}%</td>
    </tr>
    """
    return html

def get_starved_table(start,end):
    plc_con = helper_functions.get_sql_conn('plc_db')
    seconds_between = (end - start).seconds
    AUTO_CLOSER_PATHS = [
                        '[3BM1_29500_01]Line1A/Closer/TSM/StateControl',
                        '[3BM2_29500_01]LINE1A/Closer/TSM/StateControl',
                        '[3BM3_29500_01]LINE1A/Closer/TSM/StateControl'
                        ]

    auto_close_df = helper_functions.query_tsm_state(plc_con,start, end, AUTO_CLOSER_PATHS, 'Starved')
    #get percentage (divide by seconds in between start and end and multiply by 100%)
    auto_close_bma1_percent = round(helper_functions.get_val(auto_close_df,'3BM1','LINE','Duration')/seconds_between*100,1)
    auto_close_bma2_percent = round(helper_functions.get_val(auto_close_df,'3BM2','LINE','Duration')/seconds_between*100,1)
    auto_close_bma3_percent = round(helper_functions.get_val(auto_close_df,'3BM3','LINE','Duration')/seconds_between*100,1)

    plc_con.close()
    
    html=f"""
        <tr>
            <td style="text-align:right"><b>Closer Starved</b></td>
            <td style="text-align:center">{auto_close_bma1_percent}%</td>
            <td style="text-align:center">{auto_close_bma2_percent}%</td>
            <td style="text-align:center">{auto_close_bma3_percent}%</td>
        </tr>
        """

    return html

def get_cycle_time_table(start,end):
    plc_con = helper_functions.get_sql_conn('plc_db')

    BANDO_CT_PATHS = [
                        '[3BM1_29500_01]BandoLandCT/CycleTimeReporting/PalletInfeed',
                        '[3BM2_29500_01]BandoLandCT/CycleTimeReporting/PalletInfeed',
                        '[3BM3_29500_01]BandoLandCT/CycleTimeReporting/PalletInfeed'
                        ]

    bando_df = helper_functions.query_tsm_cycle_time(plc_con,start,end,BANDO_CT_PATHS)
    bando_ct_bma1 = round(helper_functions.get_val(bando_df,'3BM1','LINE','CT_SEC'),1)
    bando_ct_bma2 = round(helper_functions.get_val(bando_df,'3BM2','LINE','CT_SEC'),1)
    bando_ct_bma3 = round(helper_functions.get_val(bando_df,'3BM3','LINE','CT_SEC'),1)

    SIDEMOUNT_CT_PATHS = [
                        '[3BM1_29500_01]ManualStationReporting/SidemountInstall/StateControl',
                        '[3BM2_29500_01]ManualStationReporting/SidemountInstall/StateControl',
                        '[3BM3_29500_01]ManualStationReporting/SidemountInstall/StateControl'
                        ]

    sidemount_df = helper_functions.query_tsm_cycle_time(plc_con,start,end,SIDEMOUNT_CT_PATHS)
    sidemount_ct_bma1 = round(helper_functions.get_val(sidemount_df,'3BM1','LINE','CT_SEC'),1)
    sidemount_ct_bma2 = round(helper_functions.get_val(sidemount_df,'3BM2','LINE','CT_SEC'),1)
    sidemount_ct_bma3 = round(helper_functions.get_val(sidemount_df,'3BM3','LINE','CT_SEC'),1)
    
    QIS_CT_PATHS = [
                        '[3BM1_29500_01]ManualStationReporting/QIS/StateControl',
                        '[3BM2_29500_01]ManualStationReporting/QIS/StateControl',
                        '[3BM3_29500_01]ManualStationReporting/QIS/StateControl'
                        ]

    qis_df = helper_functions.query_tsm_cycle_time(plc_con,start,end,QIS_CT_PATHS)
    qis_ct_bma1 = round(helper_functions.get_val(qis_df,'3BM1','LINE','CT_SEC'),1)
    qis_ct_bma2 = round(helper_functions.get_val(qis_df,'3BM2','LINE','CT_SEC'),1)
    qis_ct_bma3 = round(helper_functions.get_val(qis_df,'3BM3','LINE','CT_SEC'),1)

    plc_con.close()
    
    html=f"""
        <tr>
            <td style="text-align:left"><b>BandoLand</b></td>
            <td style="text-align:center">{bando_ct_bma1}</td>
            <td style="text-align:center">{bando_ct_bma2}</td>
            <td style="text-align:center">{bando_ct_bma3}</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>Sidemount</b></td>
            <td style="text-align:center">{sidemount_ct_bma1}</td>
            <td style="text-align:center">{sidemount_ct_bma2}</td>
            <td style="text-align:center">{sidemount_ct_bma3}</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>QIS</b></td>
            <td style="text-align:center">{qis_ct_bma1}</td>
            <td style="text-align:center">{qis_ct_bma2}</td>
            <td style="text-align:center">{qis_ct_bma3}</td>
        </tr>
        """
    return html

def main(env,eos=False):
    #define start and end time for the hour
    lookback=12 if eos else 1
    now=datetime.utcnow()
    logging.info("Output123 start %s" % datetime.utcnow())
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    logging.info(str(start))
    logging.info(str(end))

    #define globals
    NORMAL_DIVISOR = 4
    CTA_DIVISOR = 28
    CTA_FLOWSTEP = '3BM-20000'
    MAMC_295_FLOWSTEP= '3BM-29500'
    MAMC_296_FLOWSTEP= '3BM-29600'
    C3A_FLOWSTEP = '3BM-40001'
    LINES = ['3BM1','3BM2','3BM3']

    #create flowstep list
    flowsteps = [CTA_FLOWSTEP,MAMC_295_FLOWSTEP,MAMC_296_FLOWSTEP,C3A_FLOWSTEP]
    #create mos connection
    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    #get output for flowsteps
    mmamc_output = get_mmamc_output(mos_con,start,end)
    df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)    

    mos_con.close()

    cta_outputs = []
    mamc_295_outputs = []
    mamc_296_outputs = []
    c3a_outputs = []
    cta1_outputs = []
    cta2_outputs = []
    cta3_outputs = []
    for line in LINES:
        cta_outputs.append(helper_functions.get_output_val(df_output,CTA_FLOWSTEP,line))
        mamc_295_outputs.append(helper_functions.get_output_val(df_output,MAMC_295_FLOWSTEP,line))
        mamc_296_outputs.append(helper_functions.get_output_val(df_output,MAMC_296_FLOWSTEP,line))
        c3a_outputs.append(helper_functions.get_output_val(df_output,C3A_FLOWSTEP,line))

    for lane in range(1,5):
        lane_num = str(lane).zfill(2)
        cta1_outputs.append(helper_functions.get_output_val(df_output,CTA_FLOWSTEP,'3BM1',actor=f"3BM1-20000-{lane_num}"))
        cta2_outputs.append(helper_functions.get_output_val(df_output,CTA_FLOWSTEP,'3BM2',actor=f"3BM2-20000-{lane_num}"))
        cta3_outputs.append(helper_functions.get_output_val(df_output,CTA_FLOWSTEP,'3BM3',actor=f"3BM3-20000-{lane_num}"))

    if mmamc_output > 0:
        header_mmamc_str = """<th style="text-align:center">MMAMC</th>"""
        blank_mmamc_str = """<td style="text-align:center">----</td>"""
        output_mmamc_str = f"""<td style="text-align:center">{mmamc_output/NORMAL_DIVISOR:.2f}</td>"""
    else:
        header_mmamc_str = ""
        blank_mmamc_str = ""
        output_mmamc_str = ""

    mamc_outputs = np.add(mamc_295_outputs, mamc_296_outputs)

    total_cta_output = helper_functions.get_output_val(df_output,CTA_FLOWSTEP)
    total_mamc_output = helper_functions.get_output_val(df_output,MAMC_295_FLOWSTEP) + helper_functions.get_output_val(df_output,MAMC_296_FLOWSTEP)
    total_c3a_output = helper_functions.get_output_val(df_output,C3A_FLOWSTEP)

    #create bma header
    bma_header_html = f"""<tr>
            <th style="text-align:center"></th>
            <th style="text-align:center">BMA1</th>
            <th style="text-align:center">BMA2</th>
            <th style="text-align:center">BMA3</th>
            {header_mmamc_str}
            <th style="text-align:center">TOTAL</th>
            <th style="text-align:center"></th>
            <th style="text-align:center"></th>
            <th style="text-align:center">Lane1</th>
            <th style="text-align:center">Lane2</th>
            <th style="text-align:center">Lane3</th>
            <th style="text-align:center">Lane4</th>
            </tr>
    """
    #create cta output row
    cta_output_html = f"""<tr>
            <td style="text-align:center"><strong>CTA</strong></td>
            <td style="text-align:center">{cta_outputs[0]/CTA_DIVISOR:.2f}</td>
            <td style="text-align:center">{cta_outputs[1]/CTA_DIVISOR:.2f}</td>
            <td style="text-align:center">{cta_outputs[2]/CTA_DIVISOR:.2f}</td>
            {blank_mmamc_str}
            <td style="text-align:center"><strong>{total_cta_output/CTA_DIVISOR:.2f}</strong></td>
            <td style="text-align:center">||</td>
            <td style="text-align:center"><strong>CTA1</strong></td>
            <td style="text-align:center">{cta1_outputs[0]/CTA_DIVISOR:.2f}</td>
            <td style="text-align:center">{cta1_outputs[1]/CTA_DIVISOR:.2f}</td>
            <td style="text-align:center">{cta1_outputs[2]/CTA_DIVISOR:.2f}</td>
            <td style="text-align:center">{cta1_outputs[3]/CTA_DIVISOR:.2f}</td>
            </tr>
    """
    
    #create mamc output row
    mamc_output_html = f"""<tr>
            <td style="text-align:center"><strong>MAMC</strong></td>
            <td style="text-align:center">{mamc_outputs[0]/NORMAL_DIVISOR:.2f}</td>
            <td style="text-align:center">{mamc_outputs[1]/NORMAL_DIVISOR:.2f}</td>
            <td style="text-align:center">{mamc_outputs[2]/NORMAL_DIVISOR:.2f}</td>
            {output_mmamc_str}
            <td style="text-align:center"><strong>{(total_mamc_output+mmamc_output)/NORMAL_DIVISOR:.2f}</strong></td>
            <td style="text-align:center">||</td>
            <td style="text-align:center"><strong>CTA2</strong></td>
            <td style="text-align:center">{cta2_outputs[0]/CTA_DIVISOR:.2f}</td>
            <td style="text-align:center">{cta2_outputs[1]/CTA_DIVISOR:.2f}</td>
            <td style="text-align:center">{cta2_outputs[2]/CTA_DIVISOR:.2f}</td>
            <td style="text-align:center">{cta2_outputs[3]/CTA_DIVISOR:.2f}</td>
            </tr>
    """
    #create c3a output row
    c3a_output_html = f"""<tr>
            <td style="text-align:center"><strong>C3A</strong></td>
            <td style="text-align:center">{c3a_outputs[0]/NORMAL_DIVISOR:.2f}</td>
            <td style="text-align:center">{c3a_outputs[1]/NORMAL_DIVISOR:.2f}</td>
            <td style="text-align:center">{c3a_outputs[2]/NORMAL_DIVISOR:.2f}</td>
            {blank_mmamc_str}
            <td style="text-align:center"><strong>{total_c3a_output/NORMAL_DIVISOR:.2f}</strong></td>
            <td style="text-align:center">||</td>
            <td style="text-align:center"><strong>CTA3</strong></td>
            <td style="text-align:center">{cta3_outputs[0]/CTA_DIVISOR:.2f}</td>
            <td style="text-align:center">{cta3_outputs[1]/CTA_DIVISOR:.2f}</td>
            <td style="text-align:center">{cta3_outputs[2]/CTA_DIVISOR:.2f}</td>
            <td style="text-align:center">{cta3_outputs[3]/CTA_DIVISOR:.2f}</td>
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

    CTA_LANE_GOAL = 1.4
    eos_multiplier = 12 if eos else 1
    goal = CTA_LANE_GOAL * eos_multiplier
    for i,val in enumerate(cta1_outputs):
        color_str = ""
        # color_str = "color:red;" if val/CTA_DIVISOR < goal else "font-weight:bold;"
        cta1_html += f"""
                    <td style="text-align:center;{color_str}">{val/CTA_DIVISOR:.2f}</td>
                    """

        # color_str = "color:red;" if cta2_outputs[i]/CTA_DIVISOR < goal else "font-weight:bold;"
        cta2_html += f"""
                    <td style="text-align:center;{color_str}">{cta2_outputs[i]/CTA_DIVISOR:.2f}</td>
                    """

        # color_str = "color:red;" if cta3_outputs[i]/CTA_DIVISOR < goal else "font-weight:bold;"
        cta3_html += f"""
                    <td style="text-align:center;{color_str}">{cta3_outputs[i]/CTA_DIVISOR:.2f}</td>
                    """

    cta1_html += "</tr>"
    cta2_html += "</tr>"
    cta3_html += "</tr>"

    #create full bma html with the above htmls
    cta_html = '<table>' + "<caption>CTA Breakdown</caption>" + cta_header_html + cta1_html + cta2_html + cta3_html + '</table>'
    
    header_html = """
                    <tr>
                    <td></td>
                    <th style="text-align:center"><strong>BMA1</strong></th>
                    <th style="text-align:center"><strong>BMA2</strong></th>
                    <th style="text-align:center"><strong>BMA3</strong></th>
                    </tr>
                """
    #get cycle time html
    starved_table = get_starved_table(start,end)
    cycle_time_table = get_cycle_time_table(start,end)
    mamc_yield_table = get_mamc_yield_table(start,end)

    starved_html = '<table>'+ header_html + starved_table + '</table>'
    cycle_time_html = '<table>' + "<caption>Cycle Time (secs)</caption>" + header_html + cycle_time_table + '</table>'
    mamc_yield_html = '<table>' + header_html + mamc_yield_table + '</table>'

    #get webhook based on environment
    webhook_key = 'teams_webhook_BMA123_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']

    #start end of shift message
    teams_msg = pymsteams.connectorcard(webhook)
    title = 'BMA123 EOS Report' if eos else 'BMA123 Hourly Update'
    teams_msg.title(title)
    teams_msg.summary('summary')
    K8S_BLUE = '#3970e4'
    TESLA_RED = '#cc0000'
    msg_color = TESLA_RED if eos else K8S_BLUE
    teams_msg.color(msg_color)
    
    #create cards for each major html
    bma_card = pymsteams.cardsection()
    bma_card.text(bma_html)
    teams_msg.addSection(bma_card)

    # cta_card = pymsteams.cardsection()
    # cta_card.text(cta_html)
    # teams_msg.addSection(cta_card)

    starved_card = pymsteams.cardsection()
    starved_card.text(starved_html)
    cycle_card = pymsteams.cardsection()
    cycle_card.text(cycle_time_html)
    yield_card = pymsteams.cardsection()
    yield_card.text(mamc_yield_html)

    teams_msg.addSection(cycle_card)
    teams_msg.addSection(starved_card)
    teams_msg.addSection(yield_card)

    teams_msg.addLinkButton("Questions?", "https://confluence.teslamotors.com/display/PRODENG/Battery+Module+Hourly+Update")
    #SEND IT
    teams_msg.send()
