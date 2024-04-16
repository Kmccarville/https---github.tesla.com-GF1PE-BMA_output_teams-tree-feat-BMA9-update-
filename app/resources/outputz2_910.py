import logging
from datetime import datetime, timedelta

from common import helper_functions as helper_functions
import numpy as np
import pandas as pd
import pymsteams
import pytz
from common.constants import K8S_BLUE, TESLA_RED, Z2_DIVISOR
from sqlalchemy import text

def get_starve_block_table(start_time,end_time):
    seconds_between = (end_time - start_time).seconds
    #define source tagpaths for each equipment type
    MAMC_ST410_PATHS = ['[3BM9_41000_BandolierLoad]Project/MDL01/TSM/StateControl']
    C3A_ST660_PATHS = ['[BM9_66000_PictureFrameHandling]Project/MDL00/TSM/StateControl']
    

    plc_con = helper_functions.get_sql_conn('plc_db')

    #get sterve blocked starve data for each tagpath set
    m_st410_df = helper_functions.query_tsm_state(plc_con,start_time, end_time, MAMC_ST410_PATHS, 'Starved')
    c_st660_df = helper_functions.query_tsm_state(plc_con,start_time, end_time, C3A_ST660_PATHS,'Blocked',reason=1)
    
    #get percentage (divide by seconds in between start and end and multiply by 100%)
    m_st410_bma9_percent = round(helper_functions.get_val(m_st410_df,'3BM9','LINE','Duration')/seconds_between*100,1)
    c_st660_bma9_percent = round(helper_functions.get_val(c_st660_df,'3BM9','LINE','Duration')/seconds_between*100,1)
    

    html=f"""
        <tr>
            <td style="text-align:right"><b>MAMC ST410</b></td>
            <td style="text-align:left">Starved by CTA</td>
            <td style="text-align:center">{m_st410_bma9_percent}%</td>
           
        </tr>
        <tr>
            <td style="text-align:right"><b>C3A ST660</b></td>
            <td style="text-align:left">Blocked by Z3</td>
            <td style="text-align:center">{c_st660_bma9_percent}%</td>
            
        </tr>
        """
    plc_con.close()
    return html, m_st410_bma9_percent, c_st660_bma9_percent

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
    plc_con.close()
    return html

def get_mamc_fpy(start_time,end_time,con):
    
    mamc_query = """select
	tp.thingid as serial,
        SUBSTRING(a.name,13,1) line,
        case 
            when nc.description IS NULL then 'pass'
            when nc.description IS NOT NULL then 'fail'
        end result
  
    FROM 
		thingpath tp 
        INNER JOIN
            actor a ON a.id = tp.actorcreatedby
        #INNER JOIN
         #   thing t ON t.id = tp.thingid
        
        LEFT JOIN
            nc ON nc.thingid = tp.thingid
            AND (nc.flowstepname in ('GFNV-BT1-3BM-42000', 'GFNV-BT1-3BM-47000', 'GFNV-BT1-3BM-48000', 'GFNV-BT1-3BM-48500') OR nc.flowstepname IS NULL)
    WHERE
        tp.completed >= """ + start_time.strftime("'%Y-%m-%d %H:%M:%S'") + """
        AND tp.completed < """ + end_time.strftime("'%Y-%m-%d %H:%M:%S'") + """
            AND tp.flowstepname = 'GFNV-BT1-3BM-42000'
        """
    
    yield_tgt_mamc = 97.0
    
    df_mamc = pd.read_sql(mamc_query, con=con)
    
    mamc_tgt_9 = "color:black:"
    
    if len(df_mamc) > 0:
       
        # line 9
        df_mamc_9 = df_mamc[df_mamc["line"]=="9"]
        if len(df_mamc_9) > 0:
            tot_mamc_9 = len(pd.unique(df_mamc_9["serial"]))
            pass_mamc_9 = len(pd.unique(df_mamc_9.loc[df_mamc_9["result"]=="pass","serial"]))
            fail_mamc_9 = len(pd.unique(df_mamc_9.loc[df_mamc_9["result"]=="fail","serial"]))
            fpy_mamc_9 = str(np.around(100 * (pass_mamc_9 / tot_mamc_9),2)) + '%'
            mamc_tgt_9 = "color:green;" if np.around(100 * (pass_mamc_9 / tot_mamc_9),2) >= yield_tgt_mamc else "color:red;"
        else:
            fpy_mamc_9 = '0'
            mamc_tgt_9 = "color:black:"
            

            
    else:
        fpy_mamc_9 = '0'
       
        mamc_tgt_9 = "color:black:"
    
    
    return fpy_mamc_9, mamc_tgt_9

def get_c3a_fpy(start_time,end_time,con):

    c3a_query = """Select 
	 tp.thingid AS 'serial',
    (CASE
        WHEN t.state = 'CONSUMED' THEN 'PASS'
        ELSE 'FAIL'
	END) AS result,
        SUBSTRING(a.name,13,1) AS line,
        SUBSTRING(a.name,22,2) AS lane,
        CASE
          WHEN SUBSTRING(a.name,15,2) = '62' THEN 'IC'
          WHEN SUBSTRING(a.name,15,2) = '63' THEN 'NIC'
        END AS assembly
from
	thingpath tp 
     INNER JOIN actor a
        ON a.id = tp.actormodifiedby
	inner join
    thing t on t.id = tp.thingid
    left join
    nc on nc.thingid = tp.thingid
    and nc.flowstepname like ('3BM%%NCM')
where
	tp.flowstepid in (1088645,
                        1099356
                        )
    and tp.completed >= """ + start_time.strftime("'%Y-%m-%d %H:%M:%S'") + """
        AND tp.completed < """ + end_time.strftime("'%Y-%m-%d %H:%M:%S'") + """
        """
    
    yield_tgt_c3a = 96.0
    
    df_c3a = pd.read_sql(c3a_query, con=con)
    
    c3a_ic_tgt_9 = "color:black:"

    c3a_nic_tgt_9 = "color:black:"

    
    if len(df_c3a) > 0:
        
        # c3a 9 ic
        df_c3a_9_ic = df_c3a[(df_c3a["line"]=="9") & (df_c3a["assembly"]=="IC")]
        
        if len(df_c3a_9_ic) > 0:
            tot_c3a_9_ic = len(pd.unique(df_c3a_9_ic["serial"]))
            pass_c3a_9_ic = len(pd.unique(df_c3a_9_ic.loc[df_c3a_9_ic["result"]=="PASS","serial"]))
            fpy_c3a_9_ic = str(np.around(100 * (pass_c3a_9_ic / tot_c3a_9_ic),2)) + '%'
            c3a_ic_tgt_9 = "color:green;" if np.around(100 * (pass_c3a_9_ic / tot_c3a_9_ic),2) >= yield_tgt_c3a else "color:red;"
        else:
            fpy_c3a_9_ic = '0'
            c3a_ic_tgt_9 = "color:black:"
            
        # c3a 9 nic
        df_c3a_9_nic = df_c3a[(df_c3a["line"]=="9") & (df_c3a["assembly"]=="NIC")]
         
        if len(df_c3a_9_nic) > 0:
            tot_c3a_9_nic = len(pd.unique(df_c3a_9_nic["serial"]))
            pass_c3a_9_nic = len(pd.unique(df_c3a_9_nic.loc[df_c3a_9_nic["result"]=="PASS","serial"]))
            fpy_c3a_9_nic = str(np.around(100 * (pass_c3a_9_nic / tot_c3a_9_nic),2)) + '%'
            c3a_nic_tgt_9 = "color:green;" if np.around(100 * (pass_c3a_9_nic / tot_c3a_9_nic),2) >= yield_tgt_c3a else "color:red;"
        else:
            fpy_c3a_9_nic = '0'
            c3a_nic_tgt_9 = "color:black:"
             
        
    else:
        fpy_c3a_9_ic = '0'
      
        fpy_c3a_9_nic= '0' 
   
        c3a_ic_tgt_9 = "color:black:"
       
        c3a_nic_tgt_9 = "color:black:"
      

    return fpy_c3a_9_ic,  fpy_c3a_9_nic, c3a_ic_tgt_9, c3a_nic_tgt_9

def main(env,eos=False):
    logging.info("Output Z2 910 start %s" % datetime.utcnow())
    lookback=12 if eos else 1
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    #define globals
    MAMC_FLOWSTEP_END= '48500'
    C3A_FLOWSTEP_END = '66000'
    LINES = ['GFNV-BT1-3BM']

    flowsteps = []
    for line in LINES:
        flowsteps.append(f"{line}-{MAMC_FLOWSTEP_END}")
        flowsteps.append(f"{line}-{C3A_FLOWSTEP_END}")

    mos_con = helper_functions.get_sql_conn('mos_rpt2',schema='sparq')
    df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)
    hourly_goal_dict = helper_functions.get_zone_line_goals(zone=2,hours=lookback)

    #get fpy for mamc
    mamc_fpy = get_mamc_fpy(start, end, mos_con)
    fpy_mamc_9, mamc_tgt_9, = mamc_fpy

    
    #get fpy for c3a
    c3a_fpy = get_c3a_fpy(start, end, mos_con)
    
    fpy_c3a_9_ic, fpy_c3a_9_nic,  c3a_ic_tgt_9, c3a_nic_tgt_9 = c3a_fpy


    mos_con.close()

    mamc_outputs = []
    c3a_outputs = []
    for line in LINES:
        mamc_outputs.append(helper_functions.get_output_val(df_output,f"{line}-{MAMC_FLOWSTEP_END}",line))
        c3a_outputs.append(helper_functions.get_output_val(df_output,f"{line}-{C3A_FLOWSTEP_END}",line))

    total_mamc9_output = helper_functions.get_output_val(df_output,f"GFNV-BT1-3BM-{MAMC_FLOWSTEP_END}")
    total_c3a9_output = helper_functions.get_output_val(df_output,f"GFNV-BT1-3BM-{C3A_FLOWSTEP_END}")

   


    #create bma header
    bma_header_html = """<tr>
            <th style="text-align:center"></th>
            <th style="text-align:center">BMA9</th>
            <th style="text-align:center">TOTAL</th>
            </tr>
    """


    #create mamc output row
    mamc_output_html = f"""<tr>
            <td style="text-align:center"><strong>MAMC</strong></td>
            <td style="text-align:center">{mamc_outputs/Z2_DIVISOR:.1f}</td>
            <td style="text-align:center"><strong>{(total_mamc9_output)/Z2_DIVISOR:.1f}</strong></td>
            </tr>
    """


    #create c3a output row
    c3a_output_html = f"""<tr>
            <td style="text-align:center"><strong>C3A</strong></td>
            <td style="text-align:center">{c3a_outputs[0]/Z2_DIVISOR:.1f}</td>
            <td style="text-align:center">{c3a_outputs[1]/Z2_DIVISOR:.1f}</td>
            <td style="text-align:center"><strong>{(total_c3a9_output)/Z2_DIVISOR:.1f}</strong></td>
            </tr>
    """
    goal_html = f"""<tr>
            <td style="text-align:center"><strong>GOAL</strong></td>
            <td style="text-align:center">{int(hourly_goal_dict['3BM9'])}</td>
     
            </tr>
    """
    #create full bma html with the above htmls
    bma_html = '<table>' + bma_header_html + mamc_output_html + c3a_output_html + goal_html + '</table>'

    mamc_starved_html, m_st10_4, m_st10_5, c3a_st120_4, c3a_st120_5 = get_starve_block_table(start,end)
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
            <th style="text-align:center">BMA9</th>
    
            </tr>
     """

    #create mamc output row
    mamc_fpy_html = f"""<tr>
             <td style="text-align:center"><strong>MAMC</strong></td>
             <td style="text-align:center;{mamc_tgt_9}">{fpy_mamc_9}</td>
             </tr>
     """

    mamc_fpy_html = "<table>" + "<caption>MAMC FPY</caption>" + mamc_fpy_header_html + mamc_fpy_html + "</table>"

    #create c3a yield header
    c3a_fpy_header_html = """<tr>
            <th style="text-align:center"></th>
            <th style="text-align:center">BMA9</th>
    
            </tr>
     """

    #create c3a ic output row
    ic_fpy_html = f"""<tr>
             <td style="text-align:center"><strong>IC</strong></td>
             <td style="text-align:center;{c3a_ic_tgt_9}">{fpy_c3a_9_ic}</td>

             </tr>
     """
     
    #create c3a nic output row
    nic_fpy_html = f"""<tr>
             <td style="text-align:center"><strong>NIC</strong></td>
             <td style="text-align:center;{c3a_nic_tgt_9}">{fpy_c3a_9_nic}</td>
             </tr>
     """

    c3a_fpy_html = "<table>" + "<caption>C3A DISPENSE FPY</caption>" + c3a_fpy_header_html + ic_fpy_html + nic_fpy_html + "</table>"    

    if env == 'prod':
        teams_con = helper_functions.get_sql_conn('pedb', schema='teams_output')
        try:
            historize_to_db(teams_con,
                            24,
                            mamc_outputs[0],
                            c3a_outputs[0],
                            int(hourly_goal_dict['3BM9']),
                            m_st10_4,
                            c3a_st120_4,
                            fpy_mamc_9,
                            fpy_c3a_9_ic,
                            fpy_c3a_9_nic,
                            eos)
            
        except Exception as e:
            logging.exception(f'Historization for z2_910 failed. See: {e}')
        teams_con.close()

    webhook_key = 'teams_webhook_BMA910_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']
    #test
    #making the hourly teams message
    teams_msg = pymsteams.connectorcard(webhook)
    title = 'BMA910 ZONE2 EOS Report' if eos else 'BMA910 ZONE2 Hourly Update'
    teams_msg.title(title)
    teams_msg.summary('summary')
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
    except pymsteams.TeamsWebhookException:
        logging.warn("Webhook timed out, retry once")
        try:
            teams_msg.send()
        except pymsteams.TeamsWebhookException:
            logging.exception("Webhook timed out twice -- pass to next area")


def historize_to_db(db, _id, mamc, c3a, c3a_mamc_goal, mamc_st10, 
                    c3a_st120, ic, nic, mamc_fpy,eos):
    sql_date = helper_functions.get_sql_pst_time()
    df_insert = pd.DataFrame({
        'LINE_ID' : [_id],
        'MAMC_OUTPUT' : [round(mamc/Z2_DIVISOR, 2) if mamc is not None else None],
        'C3A_OUTPUT' : [round(c3a/Z2_DIVISOR, 2) if c3a is not None else None],
        'C3A_MAMC_GOAL' : [round(c3a_mamc_goal, 2) if c3a_mamc_goal is not None else None],
        'STARVATION_MAMC_ST10_PERCENT' : [mamc_st10 if  mamc_st10 is not None else None],
        'BLOCKED_C3A_ST120_PERCENT' : [c3a_st120 if c3a_st120 is not None else None],
        'MAMC_FPY_PERCENT' : [round(float(mamc_fpy.replace('%', '')), 2) if mamc_fpy is not None else None],
        'C3A_DISPENSE_IC_FPY_PERCENT' : [round(float(ic.replace('%', '')), 2) if ic is not None else None],
        'C3A_DISPENSE_NIC_FPY_PERCENT': [round(float(nic.replace('%', '')), 2) if nic is not None else None],
        'END_TIME': [sql_date],
        'END_OF_SHIFT' : [int(eos)]
    }, index=['line'])
    
    df_insert.to_sql('zone2_bma910', con=db, if_exists='append', index=False)
