import common.helper_functions as helper_functions
from datetime import datetime
from datetime import timedelta
import logging
from sqlalchemy import text
import pandas as pd
import numpy as np
import pymsteams

def get_starve_block_table(start_time,end_time):
    seconds_between = (end_time - start_time).seconds
    #define source tagpaths for each equipment type
    MAMC_ST10_PATHS = ['[3BM4_30000_Ingress]Project/MDL10/Gripper/Sequences/SeqGripper','[_3BM5_30000_Ingress]Project/MDL10/Gripper/Sequences/SeqGripper']

    plc_con = helper_functions.get_sql_conn('plc_db')

    #get sterve blocked starve data for each tagpath set
    m_st10_df = helper_functions.query_tsm_state(plc_con,start_time, end_time, MAMC_ST10_PATHS, 'Starved')

    plc_con.close()

    #get percentage (divide by seconds in between start and end and multiply by 100%)
    m_st10_bma4_percent = round(helper_functions.get_val(m_st10_df,'3BM4','LINE','Duration')/seconds_between*100,1)
    m_st10_bma5_percent = round(helper_functions.get_val(m_st10_df,'3BM5','LINE','Duration')/seconds_between*100,1)

    html=f"""
        <tr>
            <td style="text-align:right"><b>MAMC ST10</b></td>
            <td style="text-align:left">Starved by CTA</td>
            <td style="text-align:center">{m_st10_bma4_percent}%</td>
            <td style="text-align:center">{m_st10_bma5_percent}%</td>
        </tr>
        """

    return html

def get_starve_by_operator(start_time,end_time):
    seconds_between = (end_time - start_time).seconds
    CELL_LOAD_PATHS = [
                    '[TSL053_CTR025_02]OEE_Reporting/TSM_CellLoad',
                    '[TSL053_CTR025_03]OEE_Reporting/TSM_CellLoad',
                    '[TSL053_CTR025_04]OEE_Reporting/TSM_CellLoad',
                    '[TSL053_CTR025_05]OEE_Reporting/TSM_CellLoad',
                    '[TSL053_CTR025_06]OEE_Reporting/TSM_CellLoad',
                    '[TSL053_CTR025_07]OEE_Reporting/TSM_CellLoad',
                    '[TSL053_CTR025_08]OEE_Reporting/TSM_CellLoad',
                    '[TSL063_CTR025_02]OEE_Reporting/TSM_CellLoad',
                    '[TSL063_CTR025_03]OEE_Reporting/TSM_CellLoad',
                    '[TSL063_CTR025_04]OEE_Reporting/TSM_CellLoad',
                    '[TSL063_CTR025_05]OEE_Reporting/TSM_CellLoad',
                    '[TSL063_CTR025_06]OEE_Reporting/TSM_CellLoad',
                    '[TSL063_CTR025_07]OEE_Reporting/TSM_CellLoad',
                    '[TSL063_CTR025_08]OEE_Reporting/TSM_CellLoad',
                    '[GFNV_CTA_008_00225_01]OEE_Reporting/TSM_CellLoad',
                    '[GFNV_CTA_008_00225_02]OEE_Reporting/TSM_CellLoad'
                    ]

    plc_con = helper_functions.get_sql_conn('plc_db')
    #get sterve blocked starve data for each tagpath set
    df = helper_functions.query_tsm_state_by_lane(plc_con,start_time, end_time, CELL_LOAD_PATHS, 'Starved',reason=1)

    plc_con.close()

    STARVED_THREHSOLD = 15

    header_html = """<tr>
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
    
    #form cta4 html
    cta4_html = f"""
                    <tr>
                        <td style="text-align:right"><strong>CTA4</strong></td>
                        <td style="text-align:center">---</td>
                    """
    
    for lane in range(2,9):
        starved_by_op = round(helper_functions.get_val(df,f'3BM4-20000-0{lane}_OEE','EQPT_NAME','Duration')/seconds_between*100,1)
        color_text = "color:red" if starved_by_op > STARVED_THREHSOLD else ""
        cta4_html += f"""<td style="text-align:center;{color_text}">{starved_by_op}%</td>"""
    
    cta4_html += "</tr>"

    #form cta5 html
    cta5_html = f"""
                    <tr>
                        <td style="text-align:right"><strong>CTA5</strong></td>
                        <td style="text-align:center">---</td>
                    """

    for lane in range(2,9):
        starved_by_op = round(helper_functions.get_val(df,f'3BM5-20000-0{lane}_OEE','EQPT_NAME','Duration')/seconds_between*100,1)
        color_text = "color:red" if starved_by_op > STARVED_THREHSOLD else ""
        cta5_html += f"""<td style="text-align:center;{color_text}">{starved_by_op}%</td>"""
    
    cta5_html += "</tr>"

    #form cta8 html
    cta8_html = f"""
                    <tr>
                        <td style="text-align:right"><strong>CTA8</strong></td>
                    """

    for lane in range(1,3):
        starved_by_op = round(helper_functions.get_val(df,f'3BM8-20000-0{lane}_OEE','EQPT_NAME','Duration')/seconds_between*100,1)
        color_text = "color:red" if starved_by_op > STARVED_THREHSOLD else ""
        cta8_html += f"""<td style="text-align:center;{color_text}">{starved_by_op}%</td>"""
    
    cta8_html +=  """
                    <td style="text-align:center">---</td>
                    <td style="text-align:center">---</td>
                    <td style="text-align:center">---</td>
                    <td style="text-align:center">---</td>
                    <td style="text-align:center">---</td>
                    <td style="text-align:center">---</td>
                </tr>
                """

    html = cta4_html + cta5_html + cta8_html
    return "<table>" + f"<caption>ST025 Cell Load Starved by USH/Operator (Goal < {STARVED_THREHSOLD}%)</caption>" +header_html+html + "</table>"

def get_cta_yield(db,lookback):
    query = f"""
        SELECT  
        a.name as LINE
            ,COUNT(DISTINCT(tp.thingid)) AS BUILT
            ,COUNT(DISTINCT(nc.thingid)) AS NC
            ,left((COUNT(DISTINCT tp.thingid) - COUNT(DISTINCT nc.thingid))*100/COUNT(DISTINCT tp.thingid) ,4) AS YIELD
        FROM sparq.thingpath tp force index
        (ix_thingpath_flowstepid_iscurrent_completed
        )
        INNER JOIN sparq.actor a
        ON a.id = tp.actorcreatedby
        LEFT JOIN
        (
            SELECT *
            FROM nc
            WHERE processname IN ('3bm4-bandolier', '3bm5-bandolier', '3bm8-bandolier')
            AND flowstepname not IN ('3bm4-25500', '3bm5-25500','3bm8-25500' )
            AND stepname IN ('3bm4-22000', '3bm4-24000', '3bm4-25000', '3bm5-22000', '3bm5-24000', '3bm5-25000' , '3bm8-25000')
        ) AS nc
        ON nc.thingid = tp.thingid
        WHERE tp.completed > NOW() - INTERVAL {lookback} HOUR
        AND tp.iscurrent = 0
        AND tp.flowstepid IN (891483, 891496, 1054858)
        GROUP BY 1
        ORDER BY 1 ASC    """
    df = pd.read_sql(text(query), db)
    return df

def cta_records(lookback,cta2,cta3,cta4,cta5,cta8,webhook):
    logging.info(f'Starting {lookback} hour ACTA records')
    # check for output records for 1 hour
    record_con = helper_functions.get_sql_conn('pedb',schema='records')

    line2 = cta2
    line3 = cta3
    line4 = cta4
    line5 = cta5
    line8 = cta8
    names = ['ACTA2','ACTA3','CTA4','CTA5','CTA8']
    carsets = [line2,line3,line4,line5,line8]
    newRecordArray = []
    prevShiftArray = []
    prevDateArray = []
    prevRecordArray = []

    for line in range(len(names)):
        newRecord, prevShift, prevDate, prevRecord = helper_functions.evaluate_record(record_con,names[line],lookback,carsets[line])
        newRecordArray.append(newRecord)
        prevShiftArray.append(prevShift)
        prevDateArray.append(prevDate)
        prevRecordArray.append(prevRecord)

    record_con.close()

    if True in np.isin(True,newRecordArray):
        for line in range(len(newRecordArray)):
            if newRecordArray[line] == True:
                lineName = names[line]
                carsPrev = prevRecordArray[line]
                shiftPrev = prevShiftArray[line]
                datePrev = prevDateArray[line]
                carsNew = carsets[line]
                shiftNow,date = helper_functions.get_shift_and_date()

                logging.info(f'Starting Record Post for {lineName}')

                html = f"""
                        <tr>
                            <th style="text-align:center"><strong></strong></th>
                            <th style="text-align:center"><strong>Shift</strong></th>
                            <th style="text-align:center"><strong>Date</strong></th>
                            <th style="text-align:center"><strong>Carsets</strong></th>
                        </tr>
                        <tr>
                            <td style="text-align:right"><b>Prev Record</b></td>
                            <td style="text-align:center">{shiftPrev}</td>
                            <td style="text-align:center">{datePrev}</td>
                            <td style="text-align:center">{carsPrev}</td>
                        </tr>
                        <tr>
                            <td style="text-align:right"><b>NEW RECORD</b></td>
                            <td style="text-align:center">{shiftNow}</td>
                            <td style="text-align:center">{date}</td>
                            <td style="text-align:center">{carsNew:.1f}</td>
                        </tr>
                        """
                output_html = '<table>' + html + '</table>'

                #making the record teams message
                record_msg = pymsteams.connectorcard(webhook)
                title = f'NEW RECORD ACHEIVED FOR {lineName} | {lookback} HOUR'
                record_msg.title(title)
                record_msg.summary('summary')
                TESLA_RED = '#cc0000'
                msg_color = TESLA_RED
                record_msg.color(msg_color)
                #make a card with the hourly data
                recordCard = pymsteams.cardsection()
                recordCard.text(output_html)

                record_msg.addSection(recordCard)
                #SEND IT
                try:
                    record_msg.send()
                except pymsteams.TeamsWebhookException:
                    logging.warn("Webhook timed out, retry once")
                    try:
                        record_msg.send()
                    except pymsteams.TeamsWebhookException:
                        logging.exception("Webhook timed out twice -- pass to next area")
    else:
        logging.info('No Record to post about')

def main(env,eos=False):
    logging.info("output Z1 start %s" % datetime.utcnow())
    lookback=12 if eos else 1
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    #define globals
    NUM_LINES = 5
    NUM_LANES = 8
    CTA_DIVISOR = 28
    CTA123_FLOWSTEP = '3BM-20000'
    CTA4_FLOWSTEP = '3BM4-25000'
    CTA5_FLOWSTEP = '3BM5-25000'
    CTA8_FLOWSTEP = '3BM8-25000'
    #create line arrays
    LINES = ['3BM2','3BM3','3BM4','3BM5','3BM8']
    FLOWSTEPS = [CTA123_FLOWSTEP,CTA123_FLOWSTEP,CTA123_FLOWSTEP,CTA4_FLOWSTEP,CTA5_FLOWSTEP,CTA8_FLOWSTEP]
    
    hourly_goal_dict = helper_functions.get_zone_line_goals(zone=1)

    mos_con = helper_functions.get_sql_conn('mos_rpt2',schema='sparq')
    df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,FLOWSTEPS)

    if eos:
        df_cta_yield = get_cta_yield(mos_con,lookback)
    mos_con.close()

    cta_outputs = []
    cta1_outputs = []
    cta2_outputs = []
    cta3_outputs = []
    cta4_outputs = []
    cta4_yield = []
    cta5_outputs = []
    cta5_yield = []
    cta8_outputs = []
    cta8_yield = []

    for line in range(NUM_LINES):
        cta_outputs.append(helper_functions.get_output_val(df_output,FLOWSTEPS[line],LINES[line]))

    for lane in range(1,NUM_LANES + 1):
        lane_num = str(lane).zfill(2)
        cta2_outputs.append(helper_functions.get_output_val(df_output, CTA123_FLOWSTEP,'3BM2',actor=f"3BM2-20000-{lane_num}"))
        cta3_outputs.append(helper_functions.get_output_val(df_output, CTA123_FLOWSTEP,'3BM3',actor=f"3BM3-20000-{lane_num}"))
        cta4_outputs.append(helper_functions.get_output_val(df_output, CTA4_FLOWSTEP,'3BM4',actor=f"3BM4-20000-{lane_num}"))
        cta5_outputs.append(helper_functions.get_output_val(df_output,CTA5_FLOWSTEP,'3BM5',actor=f"3BM5-20000-{lane_num}"))
        cta8_outputs.append(helper_functions.get_output_val(df_output,CTA8_FLOWSTEP,'3BM8',actor=f"3BM8-20000-{lane_num}"))
        if eos:
            cta4_yield.append(helper_functions.get_val(df_cta_yield, f"3BM4-20000-{lane_num}",'LINE','YIELD'))
            cta5_yield.append(helper_functions.get_val(df_cta_yield, f"3BM5-20000-{lane_num}",'LINE','YIELD'))
            cta8_yield.append(helper_functions.get_val(df_cta_yield, f"3BM8-20000-{lane_num}",'LINE','YIELD'))
    cta_total = np.sum(cta2_outputs) + np.sum(cta3_outputs) + np.sum(cta4_outputs) + np.sum(cta5_outputs) + np.sum(cta8_outputs)

    #create html outp9ut
    header_html = """<tr>
                        <th style="text-align:center"></th>
                        <th style="text-align:center">TOTAL</th>
                        <th style="text-align:center">GOAL</th>
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
    cta2_html = f"""
                <tr>
                    <td style="text-align:right"><strong>CTA2</strong></td>
                    <td style="text-align:center"><strong>{np.sum(cta2_outputs)/CTA_DIVISOR:.1f}</td>
                    <td style="text-align:center"><strong>{int(hourly_goal_dict['3BM2'])}</td>
                """
    cta3_html = f"""
                <tr>
                    <td style="text-align:right"><strong>CTA3</strong></td>
                    <td style="text-align:center"><strong>{np.sum(cta3_outputs)/CTA_DIVISOR:.1f}</td>
                    <td style="text-align:center"><strong>{int(hourly_goal_dict['3BM3'])}</td>
                """
    cta4_html = f"""
                <tr>
                    <td style="text-align:right"><strong>CTA4</strong></td>
                    <td style="text-align:center"><strong>{np.sum(cta4_outputs)/CTA_DIVISOR:.1f}</td>
                    <td style="text-align:center"><strong>{int(hourly_goal_dict['3BM4'])}</td>
                    <td style="text-align:center">---</td>
                """
    cta4_yield_html = f"""
                <tr>
                    <td style="text-align:right">YIELD %</strong></td>
                    <td style="text-align:center">---</td>
                    <td style="text-align:center">---</td>
                """
    cta5_html = f"""
            <tr>
                <td style="text-align:right"><strong>CTA5</strong></td>
                <td style="text-align:center"><strong>{np.sum(cta5_outputs)/CTA_DIVISOR:.1f}</td>
                <td style="text-align:center"><strong>{int(hourly_goal_dict['3BM5'])}</td>
                <td style="text-align:center">---</td>
            """
    cta5_yield_html = f"""
            <tr>
                <td style="text-align:right">YIELD %</strong></td>
                <td style="text-align:center">---</td>
                <td style="text-align:center">---</td>
            """
    cta8_html = f"""
            <tr>
                <td style="text-align:right"><strong>CTA8</strong></td>
                <td style="text-align:center"><strong>{np.sum(cta8_outputs)/CTA_DIVISOR:.1f}</td>
                <td style="text-align:center"><strong>{int(hourly_goal_dict['3BM8'])}</td>
            """
    cta8_yield_html = f"""
            <tr>
                <td style="text-align:right">YIELD %</strong></td>
                <td style="text-align:center">---</td>
            """
    
    zone1_combined = f"""
            <tr>
            <td style="text-align:right"><strong>ZONE1</strong></td>
            <td style="text-align:center"><strong>{cta_total/CTA_DIVISOR:.1f}</td>
            """

    nolane_html = f"""<td style="text-align:center">---</td>"""

    for i,val in enumerate(cta3_outputs):
        if i in range(4):
            color_str = ""
            cta2_html += f"""
                        <td style="text-align:center">{cta2_outputs[i]/CTA_DIVISOR:.1f}</td>
                        """
            cta3_html += f"""
                        <td style="text-align:center">{cta3_outputs[i]/CTA_DIVISOR:.1f}</td>
                        """
            #cta5 - ignore first index
            if i > 0:
                color_str = ""
                cta4_html += f"""
                            <td style="text-align:center">{cta4_outputs[i]/CTA_DIVISOR:.1f}</td>
                            """
                cta5_html += f"""
                            <td style="text-align:center">{cta5_outputs[i]/CTA_DIVISOR:.1f}</td>
                            """
                if eos:
                    cta4_yield_html += f"""
                                <td style="text-align:center;{color_str}">{cta4_yield[i]}</td>
                                """
                    cta5_yield_html += f"""
                                <td style="text-align:center;{color_str}">{cta5_yield[i]}</td>
                                """
            #cta8 has 2 lanes
            if i < 2:
                color_str = ""
                cta8_html += f"""
                            <td style="text-align:center">{cta8_outputs[i]/CTA_DIVISOR:.1f}</td>
                            """
                if eos:
                    cta8_yield_html += f"""
                                <td style="text-align:center;{color_str}">{cta8_yield[i]}</td>
                                """
            else:
                color_str = ""
                cta8_html += f"""
                            <td style="text-align:center">---</td>
                            """
                if eos:
                    cta8_yield_html += f"""
                                <td style="text-align:center;{color_str}">---</td>
                                """
        else:
            cta2_html += nolane_html
            cta3_html += nolane_html
            cta4_html += f"""
                        <td style="text-align:center">{cta4_outputs[i]/CTA_DIVISOR:.1f}</td>
                        """
            cta5_html += f"""
                        <td style="text-align:center;{color_str}">{cta5_outputs[i]/CTA_DIVISOR:.1f}</td>
                        """
            cta8_html += nolane_html
            if eos:
                cta4_yield_html += f"""
                            <td style="text-align:center;{color_str}">{cta4_yield[i]}</td>
                            """
                cta5_yield_html += f"""
                            <td style="text-align:center;{color_str}">{cta5_yield[i]}</td>
                            """
                cta8_yield_html += f"""
                            <td style="text-align:center;{color_str}">---</td>
                            """

    #finish table
    cta2_html += "</tr>"
    cta3_html += "</tr>"
    cta4_html += "</tr>"
    cta4_yield_html += "</tr>"
    cta5_html += "</tr>"
    cta5_yield_html += "</tr>"
    cta8_html += "</tr>"
    cta8_yield_html += "</tr>"

    if eos:
        cta_html = '<table>' + header_html +cta2_html + cta3_html + cta4_html + cta4_yield_html + cta5_html + cta5_yield_html + cta8_html + cta8_yield_html + zone1_combined + '</table>'
    else:
        cta_html = '<table>' + header_html + cta2_html + cta3_html + cta4_html + cta5_html + cta8_html + zone1_combined + '</table>'

    op_starved_html = get_starve_by_operator(start,end)
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
    
    webhook_key = 'teams_webhook_Zone1_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']
    
    #making the hourly teams message
    teams_msg = pymsteams.connectorcard(webhook)
    title = 'Zone1 EOS Report' if eos else 'Zone1 Hourly Update'
    teams_msg.title(title)
    teams_msg.summary('summary')
    K8S_BLUE = '#3970e4'
    TESLA_RED = '#cc0000'
    msg_color = TESLA_RED if eos else K8S_BLUE
    teams_msg.color(msg_color)
    #make a card with the hourly data
    output_card = pymsteams.cardsection()
    output_card.text(cta_html)
    
    operator_card = pymsteams.cardsection()
    operator_card.text(op_starved_html)
    
    tsm_card = pymsteams.cardsection()
    tsm_card.text(tsm_html)

    # teams_msg.addSection(summary_card)
    teams_msg.addSection(output_card)
    teams_msg.addSection(operator_card)
    teams_msg.addSection(tsm_card)
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

    # do records
    cta2 = np.sum(cta2_outputs)/CTA_DIVISOR
    cta3 = np.sum(cta3_outputs)/CTA_DIVISOR
    cta4 = np.sum(cta4_outputs)/CTA_DIVISOR
    cta5 = np.sum(cta5_outputs)/CTA_DIVISOR
    cta8 = np.sum(cta8_outputs)/CTA_DIVISOR
    webhook_key = 'teams_webhook_Zone1_Records' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']

    cta_records(lookback,cta2,cta3,cta4,cta5,cta8,webhook)
