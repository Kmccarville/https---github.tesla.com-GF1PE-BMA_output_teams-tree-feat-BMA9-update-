import common.helper_functions as helper_functions
from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import pymsteams

def get_starve_block_table(start_time,end_time):
    seconds_between = (end_time - start_time).seconds
    #define source tagpaths for each equipment type
    MAMC_ST10_PATHS = ['[3BM4_30000_Ingress]Project/MDL10/Gripper/Sequences/SeqGripper','[_3BM5_30000_Ingress]Project/MDL10/Gripper/Sequences/SeqGripper']
    MAMC_ST20_PATHS = ['[3BM4_31000_CouplerStripInstall]Project/MDL10/EM Seq/StripGripper/EM_Sequence','[_3BM5_31000_CouplerStripInstall]Project/MDL10/EM Seq/StripGripper/EM_Sequence']
    MAMC_ST30_WALK_PATHS = ['[3BM4_33000_ModuleClose_NewVersion]Project/MDL00/State Machine Summary/MDL01/StateControl','[_3BM5_33000_ModuleClose_NewVersion]Project/MDL00/State Machine Summary/MDL01/StateControl']
    MAMC_ST30_FIXTURE_PATHS = ['[3BM4_33000_ModuleClose_NewVersion]Project/MDL00/State Machine Summary/MDL10/StateControl','[_3BM5_33000_ModuleClose_NewVersion]Project/MDL00/State Machine Summary/MDL10/StateControl']
    C3A_ST70_ROBOT_PATHS = ['[3BM4_46000]Project/TSL055_ST070/MDL10/TSM/StateControl','[TSL065_ST070]Project/TSL065_ST070/MDL10/TSM/StateControl']

    
    plc_con = helper_functions.get_sql_conn('plc_db')

    #get sterve blocked starve data for each tagpath set
    m_st10_df = helper_functions.query_tsm_state(plc_con,start_time, end_time, MAMC_ST10_PATHS, 'Starved')
    c_st70_df = helper_functions.query_tsm_state(plc_con,start_time, end_time, C3A_ST70_ROBOT_PATHS, 'Blocked',reason=2)

    #get percentage (divide by seconds in between start and end and multiply by 100%)
    m_st10_bma4_percent = round(helper_functions.get_val(m_st10_df,'3BM4','LINE','Duration')/seconds_between*100,1)
    m_st10_bma5_percent = round(helper_functions.get_val(m_st10_df,'3BM5','LINE','Duration')/seconds_between*100,1)
    c_st70_bma4_percent = round(helper_functions.get_val(c_st70_df,'3BM4','LINE','Duration')/seconds_between*100,1)
    c_st70_bma5_percent = round(helper_functions.get_val(c_st70_df,'3BM5','LINE','Duration')/seconds_between*100,1)

    html=f"""
        <tr>
            <td style="text-align:right"><b>MAMC ST10</b></td>
            <td style="text-align:left">Starved by CTA</td>
            <td style="text-align:center">{m_st10_bma4_percent}%</td>
            <td style="text-align:center">{m_st10_bma5_percent}%</td>
        </tr>
        <tr>
            <td style="text-align:right"><b>C3A ST70</b></td>
            <td style="text-align:left">Blocked by Module Flip</td>
            <td style="text-align:center">{c_st70_bma4_percent}%</td>
            <td style="text-align:center">{c_st70_bma5_percent}%</td>
        </tr>
        """

    return html

def get_blocked_table(start_time,end_time):
    seconds_between = (end_time - start_time).seconds
    #define source tagpaths for each equipment type
    ST50_PATHS = ['[TSL053_CTR050]Project/PLC_10/MDL10/EmSeq/CycleStateHistory/fbHistoryEM','[TSL063_CTR050]Project/PLC_10/MDL10/EmSeq/CycleStateHistory/fbHistoryEM']
    plc_con = helper_functions.get_sql_conn('plc_db')

    #get blocked data for each tagpath set
    st50_df = helper_functions.query_tsm_state(plc_con,start_time, end_time, ST50_PATHS, 'Blocked',3100001)
    #get blocked percentage (divide by 3600s and multiply by 100%)
    st50_bma4_percent = round(helper_functions.get_val(st50_df,'3BM4','LINE','Duration')/seconds_between*100,1)
    st50_bma5_percent = round(helper_functions.get_val(st50_df,'3BM5','LINE','Duration')/seconds_between*100,1)

    html=f"""
        <tr>
            <td style="text-align:left"><b>CTA Blocked</b></td>
            <td style="text-align:center">{st50_bma4_percent}%</td>
            <td style="text-align:center">{st50_bma5_percent}%</td>
        </tr>
        """
    return html

def main(env,eos=False):
    logging.info("output45 start %s" % datetime.utcnow())
    lookback=12 if eos else 1
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    #define globals
    NORMAL_DIVISOR = 4
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
        cta_outputs.append(helper_functions.get_output_val(df_output,f"{line}-{CTA_FLOWSTEP_END}",line))
        mamc_outputs.append(helper_functions.get_output_val(df_output,f"{line}-{MAMC_FLOWSTEP_END}",line))
        c3a_outputs.append(helper_functions.get_output_val(df_output,f"{line}-{C3A_FLOWSTEP_END}",line))

    for lane in range(1,9):
        lane_num = str(lane).zfill(2)
        cta4_outputs.append(helper_functions.get_output_val(df_output,f"3BM4-{CTA_FLOWSTEP_END}",'3BM4',actor=f"3BM4-20000-{lane_num}"))
        cta5_outputs.append(helper_functions.get_output_val(df_output,f"3BM5-{CTA_FLOWSTEP_END}",'3BM5',actor=f"3BM5-20000-{lane_num}"))

    total_cta4_output = helper_functions.get_output_val(df_output,f"3BM4-{CTA_FLOWSTEP_END}")
    total_mamc4_output = helper_functions.get_output_val(df_output,f"3BM4-{MAMC_FLOWSTEP_END}")
    total_c3a4_output = helper_functions.get_output_val(df_output,f"3BM4-{C3A_FLOWSTEP_END}")

    total_cta5_output = helper_functions.get_output_val(df_output,f"3BM5-{CTA_FLOWSTEP_END}")
    total_mamc5_output = helper_functions.get_output_val(df_output,f"3BM5-{MAMC_FLOWSTEP_END}")
    total_c3a5_output = helper_functions.get_output_val(df_output,f"3BM5-{C3A_FLOWSTEP_END}")

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
            <td style="text-align:center">{cta_outputs[0]/CTA_DIVISOR:.1f}</td>
            <td style="text-align:center">{cta_outputs[1]/CTA_DIVISOR:.1f}</td>
            <td style="text-align:center"><strong>{(total_cta4_output+total_cta5_output)/CTA_DIVISOR:.1f}</strong></td>
            </tr>
    """
    #create mamc output row
    mamc_output_html = f"""<tr>
            <td style="text-align:center"><strong>MAMC</strong></td>
            <td style="text-align:center">{mamc_outputs[0]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{mamc_outputs[1]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center"><strong>{(total_mamc4_output + total_mamc5_output)/NORMAL_DIVISOR:.1f}</strong></td>
            </tr>
    """
    #create c3a output row
    c3a_output_html = f"""<tr>
            <td style="text-align:center"><strong>C3A</strong></td>
            <td style="text-align:center">{c3a_outputs[0]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center">{c3a_outputs[1]/NORMAL_DIVISOR:.1f}</td>
            <td style="text-align:center"><strong>{(total_c3a4_output + total_c3a5_output)/NORMAL_DIVISOR:.1f}</strong></td>
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

    cta4_html = """
                <tr>
                   <td style="text-align:left"><strong>CTA4</strong></td>
                """
    cta5_html = """
            <tr>
            <td style="text-align:left"><strong>CTA5</strong></td>
            <td style="text-align:center">---</td>
            """

    CTA_LANE_GOAL = 3.5
    eos_multiplier = 12 if eos else 1
    goal = CTA_LANE_GOAL * eos_multiplier
    for i,val in enumerate(cta4_outputs):
        #cta4
        # color_str = "color:red;" if val/CTA_DIVISOR < goal else "font-weight:bold;"
        color_str = ""
        cta4_html += f"""
                    <td style="text-align:center;{color_str}">{val/CTA_DIVISOR:.1f}</td>
                    """
        #cta5 - ignore first index
        if i > 0:
            # color_str = "color:red;" if cta5_outputs[i]/CTA_DIVISOR < goal else "font-weight:bold;"
            color_str = ""
            cta5_html += f"""
                        <td style="text-align:center;{color_str}">{cta5_outputs[i]/CTA_DIVISOR:.1f}</td>
                        """

    cta4_html += "</tr>"
    cta5_html += "</tr>"
    
    cta_html = '<table>' + "<caption>CTA Breakdown</caption>" + cta_header_html + cta4_html + cta5_html + '</table>'
    mamc_starved_html = get_starve_block_table(start,end)
    # cta_blocked_html = get_blocked_table(start,end)
    tsm_header_html = """
                        <tr>
                        <td></td>
                        <th style="text-align:left"><strong>REASON</strong></th>
                        <th style="text-align:center"><strong>BMA4</strong></th>
                        <th style="text-align:center"><strong>BMA5</strong></th>
                        </tr>
                    """
    tsm_html = "<table>" + "<caption>Performance</caption>" + tsm_header_html + mamc_starved_html + "</table>"
    
    webhook_key = 'teams_webhook_BMA45_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']
    
    #making the hourly teams message
    teams_msg = pymsteams.connectorcard(webhook)
    title = 'BMA45 EOS Report' if eos else 'BMA45 Hourly Update'
    teams_msg.title(title)
    teams_msg.summary('summary')
    K8S_BLUE = '#3970e4'
    TESLA_RED = '#cc0000'
    msg_color = TESLA_RED if eos else K8S_BLUE
    teams_msg.color(msg_color)
    #make a card with the hourly data
    summary_card = pymsteams.cardsection()
    summary_card.text(bma_html)

    cta_card = pymsteams.cardsection()
    cta_card.text(cta_html)

    tsm_card = pymsteams.cardsection()
    tsm_card.text(tsm_html)

    teams_msg.addSection(summary_card)
    teams_msg.addSection(cta_card)
    teams_msg.addSection(tsm_card)
    teams_msg.addLinkButton("Questions?", "https://confluence.teslamotors.com/display/PRODENG/Battery+Module+Hourly+Update")
    teams_msg.send()