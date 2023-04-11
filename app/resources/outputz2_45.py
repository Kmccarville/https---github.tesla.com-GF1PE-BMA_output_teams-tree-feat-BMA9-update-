import common.helper_functions as helper_functions
from datetime import datetime
from datetime import timedelta
import logging
from sqlalchemy import text
import pandas as pd
import pymsteams
import numpy as np

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

def get_mamc_fpy(start_time,end_time,con):
    
    mamc_query = """SELECT 
        t.name serial,
        convert_tz(tp.completed,'UTC','US/Pacific') date,
        SUBSTRING(a.name,4,1) line,
    	if(nc.description like '3BM%%', SUBSTRING(nc.description, 12), nc.description) nc,
    	nc.flowstepname,
        case 
            when nc.description IS NULL then 'pass'
            when nc.description IS NOT NULL then 'fail'
        end result
    FROM thingpath tp FORCE INDEX (IX_THINGPATH_FLOWSTEPID_ISCURRENT_COMPLETED)
        INNER JOIN
            actor a ON a.id = tp.actorcreatedby
        INNER JOIN
            thing t ON t.id = tp.thingid
        
        LEFT JOIN
            nc ON nc.thingid = t.id
    WHERE
        tp.completed >= """ + start_time.strftime("'%Y-%m-%d %H:%M:%S'") + """
        AND tp.completed < """ + end_time.strftime("'%Y-%m-%d %H:%M:%S'") + """
            AND tp.flowstepid IN (840678, 849852)
            AND (nc.flowstepname in ('3BM4-34000','3BM4-40100','3BM5-34000','3BM5-40100') OR nc.flowstepname IS NULL)
        """
    
    yield_tgt_mamc = 97.0
    
    df_mamc = pd.read_sql(mamc_query, con=con)
    
    mamc_tgt_4 = "color:black:"
    mamc_tgt_5 = "color:black:"
    
    if len(df_mamc) > 0:
       
        # line 4
        df_mamc_4 = df_mamc[df_mamc["line"]=="4"]
        if len(df_mamc_4) > 0:
            tot_mamc_4 = len(pd.unique(df_mamc_4["serial"]))
            pass_mamc_4 = len(pd.unique(df_mamc_4.loc[df_mamc_4["result"]=="pass","serial"]))
            fail_mamc_4 = len(pd.unique(df_mamc_4.loc[df_mamc_4["result"]=="fail","serial"]))
            fpy_mamc_4 = str(np.around(100 * (pass_mamc_4 / tot_mamc_4),2)) + '%'
            mamc_tgt_4 = "color:green;" if np.around(100 * (pass_mamc_4 / tot_mamc_4),2) >= yield_tgt_mamc else "color:red;"
        else:
            fpy_mamc_4 = '0'
            mamc_tgt_4 = "color:black:"
            
        # line 5
        df_mamc_5 = df_mamc[df_mamc["line"]=="5"]
        if len(df_mamc_5) > 0:
            tot_mamc_5 = len(pd.unique(df_mamc_5["serial"]))
            pass_mamc_5 = len(pd.unique(df_mamc_5.loc[df_mamc_5["result"]=="pass","serial"]))
            fail_mamc_5 = len(pd.unique(df_mamc_5.loc[df_mamc_5["result"]=="fail","serial"]))
            fpy_mamc_5 = str(np.around(100 * (pass_mamc_5 / tot_mamc_5),2)) + '%'
            mamc_tgt_5 = "color:green;" if np.around(100 * (pass_mamc_5 / tot_mamc_5),2) >= yield_tgt_mamc else "color:red;"
        else: 
            fpy_mamc_5 = '0'
            mamc_tgt_5 = "color:black:"
            
    else:
        fpy_mamc_4 = '0'
        fpy_mamc_5 = '0' 
        mamc_tgt_4 = "color:black:"
        mamc_tgt_5 = "color:black:"
    
    return fpy_mamc_4, fpy_mamc_5, mamc_tgt_4, mamc_tgt_5

def get_c3a_fpy(start_time,end_time,con):

    c3a_query = """Select 
	 tp.thingid AS 'serial',
        case 
		when nc.description IS NULL then 'PASS'
		when nc.description IS NOT NULL then 'FAIL'
	end  as result,
        SUBSTRING(a.name,4,1) AS line,
        SUBSTRING(a.name,13,2) AS lane,
        CASE
          WHEN SUBSTRING(a.name,6,2) = '41' THEN 'IC'
          WHEN SUBSTRING(a.name,6,2) = '43' THEN 'NIC'
        END AS assembly
from
	thingpath tp 
     INNER JOIN actor a
        ON a.id = tp.actormodifiedby
    left join
    nc on nc.thingid = tp.thingid
    and nc.createdby like ('3BM%%')
where
	tp.flowstepid in (817280,
						819346,
                        845623,
                        1017879)
    and tp.completed >= """+start_time.strftime("'%Y-%m-%d %H:%M:%S'")+"""
        AND tp.completed < """+end_time.strftime("'%Y-%m-%d %H:%M:%S'")+"""
        """
    
    yield_tgt_c3a = 96.0
    
    df_c3a = pd.read_sql(c3a_query, con=con)
    
    c3a_ic_tgt_4 = "color:black:"
    c3a_ic_tgt_5 = "color:black:"
    c3a_nic_tgt_4 = "color:black:"
    c3a_nic_tgt_5 = "color:black:"
    
    if len(df_c3a) > 0:
        
        # c3a 4 ic
        df_c3a_4_ic = df_c3a[(df_c3a["line"]=="4") & (df_c3a["assembly"]=="IC")]
        
        if len(df_c3a_4_ic) > 0:
            tot_c3a_4_ic = len(pd.unique(df_c3a_4_ic["serial"]))
            pass_c3a_4_ic = len(pd.unique(df_c3a_4_ic.loc[df_c3a_4_ic["result"]=="PASS","serial"]))
            fpy_c3a_4_ic = str(np.around(100 * (pass_c3a_4_ic / tot_c3a_4_ic),2)) + '%'
            c3a_ic_tgt_4 = "color:green;" if np.around(100 * (pass_c3a_4_ic / tot_c3a_4_ic),2) >= yield_tgt_c3a else "color:red;"
        else:
            fpy_c3a_4_ic = '0'
            c3a_ic_tgt_4 = "color:black:"
            
        # c3a 4 nic
        df_c3a_4_nic = df_c3a[(df_c3a["line"]=="4") & (df_c3a["assembly"]=="NIC")]
         
        if len(df_c3a_4_nic) > 0:
            tot_c3a_4_nic = len(pd.unique(df_c3a_4_nic["serial"]))
            pass_c3a_4_nic = len(pd.unique(df_c3a_4_nic.loc[df_c3a_4_nic["result"]=="PASS","serial"]))
            fpy_c3a_4_nic = str(np.around(100 * (pass_c3a_4_nic / tot_c3a_4_nic),2)) + '%'
            c3a_nic_tgt_4 = "color:green;" if np.around(100 * (pass_c3a_4_nic / tot_c3a_4_nic),2) >= yield_tgt_c3a else "color:red;"
        else:
            fpy_c3a_4_nic = '0'
            c3a_nic_tgt_4 = "color:black:"
             
        # c3a 5 ic
        df_c3a_5_ic = df_c3a[(df_c3a["line"]=="5") & (df_c3a["assembly"]=="IC")]
        
        if len(df_c3a_5_ic) > 0:
            tot_c3a_5_ic = len(pd.unique(df_c3a_5_ic["serial"]))
            pass_c3a_5_ic = len(pd.unique(df_c3a_5_ic.loc[df_c3a_5_ic["result"]=="PASS","serial"]))
            fpy_c3a_5_ic = str(np.around(100 * (pass_c3a_5_ic / tot_c3a_5_ic),2)) + '%'
            c3a_ic_tgt_5 = "color:green;" if np.around(100 * (pass_c3a_5_ic / tot_c3a_5_ic),2) >= yield_tgt_c3a else "color:red;"
        else:
            fpy_c3a_5_ic = '0'
            c3a_ic_tgt_5 = "color:black:"
            
        # c3a 5 nic
        df_c3a_5_nic = df_c3a[(df_c3a["line"]=="5") & (df_c3a["assembly"]=="NIC")]
         
        if len(df_c3a_5_nic) > 0:
            tot_c3a_5_nic = len(pd.unique(df_c3a_5_nic["serial"]))
            pass_c3a_5_nic = len(pd.unique(df_c3a_5_nic.loc[df_c3a_5_nic["result"]=="PASS","serial"]))
            fpy_c3a_5_nic = str(np.around(100 * (pass_c3a_5_nic / tot_c3a_5_nic),2)) + '%'
            c3a_nic_tgt_5 = "color:green;" if np.around(100 * (pass_c3a_5_nic / tot_c3a_5_nic),2) >= yield_tgt_c3a else "color:red;"
        else:
            fpy_c3a_5_nic = '0'
            c3a_nic_tgt_5 = "color:black:"
             
    else:
        fpy_c3a_4_ic = '0'
        fpy_c3a_5_ic = '0'
        fpy_c3a_4_nic= '0' 
        fpy_c3a_5_nic = '0'
        c3a_ic_tgt_4 = "color:black:"
        c3a_ic_tgt_5 = "color:black:"
        c3a_nic_tgt_4 = "color:black:"
        c3a_nic_tgt_5 = "color:black:"

    return fpy_c3a_4_ic, fpy_c3a_5_ic, fpy_c3a_4_nic, fpy_c3a_5_nic, c3a_ic_tgt_4, c3a_ic_tgt_5, c3a_nic_tgt_4, c3a_nic_tgt_5

def main(env,eos=False):
    logging.info("Output Z2 45 start %s" % datetime.utcnow())
    lookback=12 if eos else 1
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    #define globals
    NORMAL_DIVISOR = 4
    MAMC_FLOWSTEP_END= '34000'
    C3A_FLOWSTEP_END = '45000'
    LINES = ['3BM4','3BM5']

    flowsteps = []
    for line in LINES:
        flowsteps.append(f"{line}-{MAMC_FLOWSTEP_END}")
        flowsteps.append(f"{line}-{C3A_FLOWSTEP_END}")

    mos_con = helper_functions.get_sql_conn('mos_rpt2',schema='sparq')
    df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)

    #get fpy for mamc
    mamc_fpy = get_mamc_fpy(start, end, mos_con)
    fpy_mamc_4, fpy_mamc_5, mamc_tgt_4, mamc_tgt_5 = mamc_fpy

    
    #get fpy for c3a
    c3a_fpy = get_c3a_fpy(start, end, mos_con)
    
    fpy_c3a_4_ic, fpy_c3a_5_ic, fpy_c3a_4_nic, fpy_c3a_5_nic, c3a_ic_tgt_4, c3a_ic_tgt_5, c3a_nic_tgt_4, c3a_nic_tgt_5 = c3a_fpy


    mos_con.close()

    mamc_outputs = []
    c3a_outputs = []
    for line in LINES:
        mamc_outputs.append(helper_functions.get_output_val(df_output,f"{line}-{MAMC_FLOWSTEP_END}",line))
        c3a_outputs.append(helper_functions.get_output_val(df_output,f"{line}-{C3A_FLOWSTEP_END}",line))

    total_mamc4_output = helper_functions.get_output_val(df_output,f"3BM4-{MAMC_FLOWSTEP_END}")
    total_c3a4_output = helper_functions.get_output_val(df_output,f"3BM4-{C3A_FLOWSTEP_END}")

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
    bma_html = '<table>' + bma_header_html + mamc_output_html + c3a_output_html + '</table>'

    mamc_starved_html = get_starve_block_table(start,end)
    tsm_header_html = """
                        <tr>
                        <td></td>
                        <th style="text-align:left"><strong>REASON</strong></th>
                        <th style="text-align:center"><strong>BMA4</strong></th>
                        <th style="text-align:center"><strong>BMA5</strong></th>
                        </tr>
                    """
    tsm_html = "<table>" + "<caption>Performance</caption>" + tsm_header_html + mamc_starved_html + "</table>"
    
    
    #create yield header
    mamc_fpy_header_html = """<tr>
            <th style="text-align:center"></th>
            <th style="text-align:center">BMA4</th>
            <th style="text-align:center">BMA5</th>
            </tr>
     """

    #create mamc output row
    mamc_fpy_html = f"""<tr>
             <td style="text-align:center"><strong>MAMC</strong></td>
             <td style="text-align:center;{mamc_tgt_4}">{fpy_mamc_4}</td>
             <td style="text-align:center;{mamc_tgt_5}">{fpy_mamc_5}</td>
             </tr>
     """

    mamc_fpy_html = "<table>" + "<caption>MAMC FPY</caption>" + mamc_fpy_header_html + mamc_fpy_html + "</table>"

    #create c3a yield header
    c3a_fpy_header_html = """<tr>
            <th style="text-align:center"></th>
            <th style="text-align:center">BMA4</th>
            <th style="text-align:center">BMA5</th>
            </tr>
     """

    #create c3a ic output row
    ic_fpy_html = f"""<tr>
             <td style="text-align:center"><strong>IC</strong></td>
             <td style="text-align:center;{c3a_ic_tgt_4}">{fpy_c3a_4_ic}</td>
             <td style="text-align:center;{c3a_ic_tgt_5}">{fpy_c3a_5_ic}</td>
             </tr>
     """
     
    #create c3a nic output row
    nic_fpy_html = f"""<tr>
             <td style="text-align:center"><strong>NIC</strong></td>
             <td style="text-align:center;{c3a_nic_tgt_4}">{fpy_c3a_4_nic}</td>
             <td style="text-align:center;{c3a_nic_tgt_5}">{fpy_c3a_5_nic}</td>
             </tr>
     """

    c3a_fpy_html = "<table>" + "<caption>C3A DISPENSE FPY</caption>" + c3a_fpy_header_html + ic_fpy_html + nic_fpy_html + "</table>"    

    webhook_key = 'teams_webhook_BMA45_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']
    
    #making the hourly teams message
    teams_msg = pymsteams.connectorcard(webhook)
    title = 'BMA45 ZONE2 EOS Report' if eos else 'BMA45 ZONE2 Hourly Update'
    teams_msg.title(title)
    teams_msg.summary('summary')
    K8S_BLUE = '#3970e4'
    TESLA_RED = '#cc0000'
    msg_color = TESLA_RED if eos else K8S_BLUE
    teams_msg.color(msg_color)
    #make a card with the hourly data
    summary_card = pymsteams.cardsection()
    summary_card.text(bma_html)

    tsm_card = pymsteams.cardsection()
    tsm_card.text(tsm_html)

    mamc_fpy_card = pymsteams.cardsection()
    mamc_fpy_card.text(mamc_fpy_html)
    
    c3a_fpy_card = pymsteams.cardsection()
    c3a_fpy_card.text(c3a_fpy_html)

    teams_msg.addSection(summary_card)
    teams_msg.addSection(tsm_card)
    teams_msg.addSection(mamc_fpy_card)
    teams_msg.addSection(c3a_fpy_card)
    teams_msg.addLinkButton("Questions?", "https://confluence.teslamotors.com/display/PRODENG/Battery+Module+Hourly+Update")
    #SEND IT
    try:
        teams_msg.send()
    except Timeout:
        logging.info("Webhook timed out, retry once")
        try:
            teams_msg.send()
        except Timeout:
            logging.info("Webhook timeded out twice -- pass to next area")
            pass