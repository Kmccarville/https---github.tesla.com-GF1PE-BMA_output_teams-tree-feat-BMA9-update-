from common.db import db_connector
import common.helper_functions as helper_functions
from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import pymsteams

def get_mamc_starved_table(start_time,end_time):
    #define source tagpaths for each equipment type
    ST10_PATHS = ['[3BM4_30000_Ingress]Project/MDL10/Gripper/Sequences/SeqGripper','[_3BM5_30000_Ingress]Project/MDL10/Gripper/Sequences/SeqGripper']
    ST20_PATHS = ['[3BM4_31000_CouplerStripInstall]Project/MDL10/EM Seq/StripGripper/EM_Sequence','[_3BM5_31000_CouplerStripInstall]Project/MDL10/EM Seq/StripGripper/EM_Sequence']
    ST30_WALK_PATHS = ['[3BM4_33000_ModuleClose_NewVersion]Project/MDL00/State Machine Summary/MDL01/StateControl','[_3BM5_33000_ModuleClose_NewVersion]Project/MDL00/State Machine Summary/MDL01/StateControl']
    ST30_FIXTURE_PATHS = ['[3BM4_33000_ModuleClose_NewVersion]Project/MDL00/State Machine Summary/MDL10/StateControl','[_3BM5_33000_ModuleClose_NewVersion]Project/MDL00/State Machine Summary/MDL10/StateControl']
    
    plc_con = helper_functions.get_sql_conn('plc_db')

    #get starveddata for each tagpath set
    st10_df = helper_functions.query_tsm_state(plc_con,start_time, end_time, ST10_PATHS, 'Starved')
    st20_df = helper_functions.query_tsm_state(plc_con,start_time, end_time, ST20_PATHS, 'Starved')
    st30_walk_df = helper_functions.query_tsm_state(plc_con,start_time, end_time, ST30_WALK_PATHS, 'Starved')
    st30_fixture_df = helper_functions.query_tsm_state(plc_con,start_time, end_time, ST30_FIXTURE_PATHS, 'Starved')

    #get starve percentage (divide by 3600s and multiply by 100%)
    st10_bma4_percent = round(helper_functions.get_val(st10_df,'3BM4','LINE','Duration')/3600*100,1)
    st10_bma5_percent = round(helper_functions.get_val(st10_df,'3BM5','LINE','Duration')/3600*100,1)
    st20_bma4_percent = round(helper_functions.get_val(st20_df,'3BM4','LINE','Duration')/3600*100,1)
    st20_bma5_percent = round(helper_functions.get_val(st20_df,'3BM5','LINE','Duration')/3600*100,1)
    st30_walk_bma4_percent = round(helper_functions.get_val(st30_walk_df,'3BM4','LINE','Duration')/3600*100,1)
    st30_walk_bma5_percent = round(helper_functions.get_val(st30_walk_df,'3BM5','LINE','Duration')/3600*100,1)
    st30_fix_bma4_percent = round(helper_functions.get_val(st30_fixture_df,'3BM4','LINE','Duration')/3600*100,1)
    st30_fix_bma5_percent = round(helper_functions.get_val(st30_fixture_df,'3BM5','LINE','Duration')/3600*100,1)

    html=f"""<table>
        <tr>
            <td>Starved %</td>
            <td style="text-align:center"><strong>MAMC4</strong></td>
            <td style="text-align:center"><strong>MAMC5</strong></td>
        </tr>
        <tr>
            <td style="text-align:left"><b>ST10-Bandoliers</b></td>
            <td style="text-align:center">{st10_bma4_percent}%</td>
            <td style="text-align:center">{st10_bma5_percent}%</td>
        </tr>
        </table>
        """
        # <tr>
        #     <td style="text-align:left"><b>ST20-Frax1</b></td>
        #     <td style="text-align:center">{st20_bma4_percent}%</td>
        #     <td style="text-align:center">{st20_bma5_percent}%</td>
        # </tr>
        # <tr>
        #     <td style="text-align:left"><b>ST30-Bando</b></td>
        #     <td style="text-align:center">{st30_walk_bma4_percent}%</td>
        #     <td style="text-align:center">{st30_walk_bma5_percent}%</td>
        # </tr>
        # <tr>
        #     <td style="text-align:left"><b>ST30-Fixture</b></td>
        #     <td style="text-align:center">{st30_fix_bma4_percent}%</td>
        #     <td style="text-align:center">{st30_fix_bma5_percent}%</td>
        # </tr>

    return html

def get_cta_output(db,start,end):
    query = f"""
            SELECT 
            tp.flowstepname as FLOWSTEP,
            right(a.name,1) as LINE,
            count(distinct tp.thingid) as OUTPUT
            FROM sparq.thingpath tp
            JOIN sparq.actor a on tp.modifiedby = a.id
            WHERE
            tp.flowstepname in ('3BM4-25000','3BM5-25000')
            AND tp.exitcompletioncode = 'PASS'
            AND tp.completed BETWEEN '{start}' AND '{end}'
            GROUP BY 1,2
            """
    df = pd.read_sql(query,db)
    return df

def get_c3a_mamc_output(db,start,end):
    query = f"""
            SELECT 
            tp.flowstepname as FLOWSTEP,
            count(distinct tp.thingid) as OUTPUT
            FROM sparq.thingpath tp
            WHERE
            tp.flowstepname in ('3BM4-34000','3BM5-34000','3BM4-45000','3BM5-45000')
            AND tp.exitcompletioncode = 'PASS'
            AND tp.completed BETWEEN '{start}' AND '{end}'
            GROUP BY 1
            """
    df = pd.read_sql(query,db)
    return df

def output45(env):
    logging.info("output45 start %s" % datetime.utcnow())
    lookback=1 #1 hr
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    #define globals
    CTA_DIVISOR = 28
    CTA_FLOWSTEP_END = '25000'
    MAMC_FLOWSTEP_END= '34000'
    C3A_FLOWSTEP_END = '45000'
    LINES = ['3BM4','3BM5']

    flowsteps = []
    for line in LINES:
        flowsteps.append(f"{line}-{CTA_FLOWSTEP_END}")
        flowsteps.append(f"{line}-{MAMC_FLOWSTEP_END}")
        flowsteps.append(f"{line}-{C3A_FLOWSTEP_END}")

    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)
    mos_con.close()

    cta_outputs = []
    mamc_outputs = []
    c3a_outputs = []
    cta4_outputs = []
    cta5_outputs = []
    for line in LINES:
        cta_outputs.append(helper_functions.get_output_val(df_output,line,f"{line}-{CTA_FLOWSTEP_END}",divisor=CTA_DIVISOR))
        mamc_outputs.append(helper_functions.get_output_val(df_output,line,f"{line}-{MAMC_FLOWSTEP_END}"))
        c3a_outputs.append(helper_functions.get_output_val(df_output,line,f"{line}-{C3A_FLOWSTEP_END}"))

    for lane in range(1,9):
        lane_num = str(lane).zfill(2)
        cta4_outputs.append(helper_functions.get_output_val(df_output,'3BM4',f"3BM4-{CTA_FLOWSTEP_END}",actor=f"3BM4-20000-{lane_num}",divisor=CTA_DIVISOR))
        cta5_outputs.append(helper_functions.get_output_val(df_output,'3BM5',f"3BM5-{CTA_FLOWSTEP_END}",actor=f"3BM5-20000-{lane_num}",divisor=CTA_DIVISOR))

    #create bma header
    bma_header_html = """<tr>
            <th style="text-align:center"></th>
            <th style="text-align:center">BMA4</th>
            <th style="text-align:center">BMA5</th>
            <th style="text-align:center">TOTAL</th>
            </tr>
    """
    #create cta output row
    cta_output_html = f"""<tr>
            <td style="text-align:center"><strong>CTA</strong></td>
            <td style="text-align:center">{cta_outputs[0]}</td>
            <td style="text-align:center">{cta_outputs[1]}</td>
            <td style="text-align:center"><strong>{round(sum(cta_outputs),1)}</strong></td>
            </tr>
    """
    #create mamc output row
    mamc_output_html = f"""<tr>
            <td style="text-align:center"><strong>MAMC</strong></td>
            <td style="text-align:center">{mamc_outputs[0]}</td>
            <td style="text-align:center">{mamc_outputs[1]}</td>
            <td style="text-align:center"><strong>{round(sum(mamc_outputs),1)}</strong></td>
            </tr>
    """
    #create c3a output row
    c3a_output_html = f"""<tr>
            <td style="text-align:center"><strong>C3A</strong></td>
            <td style="text-align:center">{c3a_outputs[0]}</td>
            <td style="text-align:center">{c3a_outputs[1]}</td>
            <td style="text-align:center"><strong>{round(sum(c3a_outputs),1)}</strong></td>
            </tr>
    """
    #create full bma html with the above htmls
    bma_html = '<table>' + bma_header_html + cta_output_html + mamc_output_html + c3a_output_html + '</table>'

    #create cta header
    cta_header_html = """<tr>
                        <th style="text-align:center"></th>
                        <th style="text-align:center">Ln1</th>
                        <th style="text-align:center">Ln2</th>
                        <th style="text-align:center">Ln3</th>
                        <th style="text-align:center">Ln4</th>
                        <th style="text-align:center">Ln5</th>
                        <th style="text-align:center">Ln6</th>
                        <th style="text-align:center">Ln7</th>
                        <th style="text-align:center">Ln8</th>
                        </tr>
                    """
    CTA_LANE_GOAL = 3.5

    cta4_html = """
                <tr>
                   <td style="text-align:left"><strong>CTA4</strong></td>
                """
    cta5_html = """
            <tr>
            <td style="text-align:left"><strong>CTA5</strong></td>
            <td style="text-align:center">---</td>
            """
    for i,val in enumerate(cta4_outputs):
        #cta4
        color_str = "color:red;" if val < CTA_LANE_GOAL else "font-weight:bold;"
        cta4_html += f"""
                    <td style="text-align:center;{color_str}">{val}</td>
                    """
        #cta5 - ignore first index
        if i > 0:
            color_str = "color:red;" if cta5_outputs[i] < CTA_LANE_GOAL else "font-weight:bold;"
            cta5_html += f"""
                        <td style="text-align:center;{color_str}">{cta5_outputs[i]}</td>
                        """

    cta4_html += "</tr>"
    cta5_html += "</tr>"
    
    cta_html = '<table>' + cta_header_html + cta4_html + cta5_html + '</table>'
    tsm_html = get_mamc_starved_table(start,end)

    webhook_key = 'teams_webhook_BMA45_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']
    
    #making the hourly teams message
    hourly_msg = pymsteams.connectorcard(webhook)
    hourly_msg.title('BMA45 Hourly Update')
    hourly_msg.summary('summary')
    hourly_msg.color('#3970e4')
    #make a card with the hourly data
    summary_card = pymsteams.cardsection()
    summary_card.text(bma_html)

    cta_card = pymsteams.cardsection()
    cta_card.text(cta_html)

    tsm_card = pymsteams.cardsection()
    tsm_card.text(tsm_html)

    hourly_msg.addSection(summary_card)
    hourly_msg.addSection(cta_card)
    hourly_msg.addSection(tsm_card)
    hourly_msg.send()