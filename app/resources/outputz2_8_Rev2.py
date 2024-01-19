import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pymsteams
from common import helper_functions


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
                    tp.flowstepname IN ('3BM8-29500')
                    AND tp.completed BETWEEN '{start}' AND '{start_next}'
                GROUP BY 1
                """
        df_sub = pd.read_sql(query,mos_con)
        df = pd.concat([df,df_sub],axis=0)
        start += timedelta(minutes=60)

    mos_con.close()

    bma8_yield = round(helper_functions.get_val(df,'3BM8','LINE','YIELD'),1)

    html=f"""
    <tr>
        <td style="text-align:center"><b>MAMC Yield</b></td>
        <td style="text-align:center">{bma8_yield}%</td>
    </tr>
    """
    return html

#Shiftly-Dispense-Yield 
def get_c3a_yield_table(start,end):
    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    df = pd.DataFrame({})
    while start < end:
        start_next = start + timedelta(minutes=60)
        query = f"""
          SELECT 
            CASE when t1.line = '3BM8-40000-01' then 'IC Dispense'
            when t1.line = '3BM8-42000-01' then 'NIC Dispense' else t1.line end as LINE,
            ((count(distinct t1.thingid) - count(distinct nc.thingid))/count(distinct t1.thingid)*100) as YIELD
            FROM (SELECT
                t.id AS thingid,
                t.name AS 'serial',
                t.state,
                fs.name AS 'flowstep',
                fsc.name AS 'current_flowstep',
                a.name AS line,
                CONVERT_TZ(tp.completed, 'UTC', 'US/Pacific') AS PST_Datetime,
                MIN(RIGHT(nc.name,1)) AS nc_id
            FROM sparq.thingpath tp
            INNER JOIN sparq.thing t
                ON t.id = tp.thingid
            INNER JOIN sparq.thingpath tpc
                ON tpc.thingid = t.id
                AND tpc.iscurrent = 1
            INNER JOIN sparq.flowstep fs
                ON fs.id = tp.flowstepid
                AND fs.name IN ('3BM8-40000','3BM8-42000')
            INNER JOIN sparq.flowstep fsc
                ON fsc.id = tpc.flowstepid
            INNER JOIN sparq.actor a
                ON a.id = tp.actorcreatedby
            LEFT JOIN sparq.nc
                ON nc.thingid = t.id
                AND nc.flowstepname LIKE ('3BM8%%')
            WHERE tp.completed BETWEEN '{start}' AND '{start_next}'
            GROUP BY 1,2,3,4,5,6,7) t1
            LEFT JOIN sparq.nc
                ON nc.thingid = t1.thingid
                AND RIGHT(nc.name,1) = t1.nc_id
                group by 1"""
        df_sub = pd.read_sql(query,mos_con)
        df = pd.concat([df,df_sub],axis=0)
        start += timedelta(minutes=60)

    mos_con.close()

    bma8_ic_dispense_yield = round(helper_functions.get_val(df,'IC Dispense','LINE','YIELD'),1)
    bma8_nic_dispense_yield = round(helper_functions.get_val(df,'NIC Dispense','LINE','YIELD'),1)

    html=f"""
    <tr>
        <td style="text-align:center"><b>C3A Dispense Yield</b></td>
        <td style="text-align:center">{bma8_ic_dispense_yield}%</td>
        <td style="text-align:center">{bma8_nic_dispense_yield}%</td>
 
    </tr>
    """
    return html

def get_performance_table(start,end):

    plc_con = helper_functions.get_sql_conn('plc_db')
    seconds_between = (end - start).seconds
    AUTO_CLOSER_PATHS = ['[_3BM08_28000_30001]Eqt_3BM08_29000_Closer/StateControl/StateControlHMI']

    auto_close_df = helper_functions.query_tsm_state(plc_con,start, end, AUTO_CLOSER_PATHS, 'Starved')
    #get percentage (divide by seconds in between start and end and multiply by 100%)
    auto_close_bma8_percent = round(helper_functions.get_val(auto_close_df,'3BM8','LINE','Duration')/seconds_between*100,1)

    P_RETURN_PATHS = ['[_3BM08_28000_30001]Eqt_3BM08_28000_PalletReturn/StateControl/StateControlHMI']
    P_RETURN_LOW_LIMIT = 38
    P_RETURN_HIGH_LIMIT = 165

    p_return_df = helper_functions.query_tsm_cycle_time(plc_con,start,end,P_RETURN_PATHS,P_RETURN_LOW_LIMIT,P_RETURN_HIGH_LIMIT)
    p_return_ct_bma8 = round(helper_functions.get_val(p_return_df,'3BM8','LINE','CT_SEC'),1)
    
    QIS_CT_PATHS = ['[_3BM08_28000_30001]Eqt_3BM08_29400_QIS/StateControl/StateControlHMI']
    QIS_LOW_LIMIT = 41
    QIS_HIGH_LIMIT = 98

    qis_df = helper_functions.query_tsm_cycle_time(plc_con,start,end,QIS_CT_PATHS,QIS_LOW_LIMIT,QIS_HIGH_LIMIT)
    qis_ct_bma8 = round(helper_functions.get_val(qis_df,'3BM8','LINE','CT_SEC'),1)

    TESTER_CT_PATHS = ['[_3BM08_28000_30001]Eqt_3BM08_29500_Tester/StateControl/StateControl_HMI']
    TESTER_LOW_LIMIT = 30
    TESTER_HIGH_LIMIT = 95

    tester_df = helper_functions.query_tsm_cycle_time(plc_con,start,end,TESTER_CT_PATHS,TESTER_LOW_LIMIT,TESTER_HIGH_LIMIT)
    tester_ct_bma8 = round(helper_functions.get_val(tester_df,'3BM8','LINE','CT_SEC'),1)


    plc_con.close()
    
    html=f"""
        <tr>
            <th style="text-align:left">Cycle Time</th>
            <th style="text-align:left">(Target:</th>
            <th style="text-align:left"> 102s)</th>
            <th></th>
            <th></th>
            <th style="text-align:left">Starved %</th>
        </tr>
        <tr>
            <td></td>
            <th style="text-align:center"><strong>BMA8</strong></th>
            <td></td>
            <td></td>
            <th style="text-align:center"><strong>BMA8</strong></th>
        </tr>
        <tr>
            <td style="text-align:left"><b>Tester</b></td>
            <td style="text-align:center">{tester_ct_bma8}</td>
            <td>||</td>
            <td style="text-align:right"><b>Auto Closer</b></td>
            <td style="text-align:center">{auto_close_bma8_percent}%</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>Pallet Return</b></td>
            <td style="text-align:center">{p_return_ct_bma8}</td>
            <td>||</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>QIS</b></td>
            <td style="text-align:center">{qis_ct_bma8}</td>
            <td>||</td>
        </tr>
        """
    
    return html

def main(env,eos=False):
    #define start and end time for the hour
    lookback=12 if eos else 1
    now=datetime.utcnow()
    logging.info("Output Z2 BMA8 start %s" % datetime.utcnow())
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    logging.info(str(start))
    logging.info(str(end))

    #define globals
    NORMAL_DIVISOR = 4
    MAMC_295_FLOWSTEP= '3BM8-29500'
    MAMC_296_FLOWSTEP= '3BM8-29600'
    C3A_FLOWSTEP = '3BM8-44000'
    LINES = ['3BM8']

    #create flowstep list
    flowsteps = [MAMC_295_FLOWSTEP,MAMC_296_FLOWSTEP,C3A_FLOWSTEP]
    #create mos connection
    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    #get output for flowsteps
    df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)    

    mos_con.close()

    mamc_295_outputs = []
    mamc_296_outputs = []
    c3a_outputs = []
    for line in LINES:
        mamc_295_outputs.append(helper_functions.get_output_val(df_output,MAMC_295_FLOWSTEP,line))
        mamc_296_outputs.append(helper_functions.get_output_val(df_output,MAMC_296_FLOWSTEP,line))
        c3a_outputs.append(helper_functions.get_output_val(df_output,C3A_FLOWSTEP,line))

    mamc_outputs = np.add(mamc_295_outputs, mamc_296_outputs)
    

    total_mamc_output = helper_functions.get_output_val(df_output,MAMC_295_FLOWSTEP) + helper_functions.get_output_val(df_output,MAMC_296_FLOWSTEP)
    total_c3a_output = helper_functions.get_output_val(df_output,C3A_FLOWSTEP)
    #create bma header
    bma_header_html = f"""<tr>
            <th style="text-align:center"></th>
            <th style="text-align:center">BMA8</th>
            <th style="text-align:center">TOTAL</th>
            </tr>
            """
    #create mamc output row
    mamc_output_html = f"""<tr>
            <td style="text-align:center"><strong>MAMC</strong></td>
            <td style="text-align:center">{mamc_outputs[0]/NORMAL_DIVISOR:.2f}</td>
            <td style="text-align:center"><strong>{total_mamc_output/NORMAL_DIVISOR:.2f}</strong></td>
            """
    #create c3a output row
    c3a_output_html = f"""<tr>
            <td style="text-align:center"><strong>C3A</strong></td>
            <td style="text-align:center">{c3a_outputs[0]/NORMAL_DIVISOR:.2f}</td>
            <td style="text-align:center"><strong>{total_c3a_output/NORMAL_DIVISOR:.2f}</strong></td>
            """

    #create full bma html with the above htmls
    bma_html = '<table>' + "<caption>Throughput</caption>" + bma_header_html + mamc_output_html + c3a_output_html + '</table>'

    #get cycle time html
    header_html = """
                    <tr>
                    <td></td>
                    <th style="text-align:center"><strong>BMA8</strong></th>
                    </tr>
                """
    # starved_table = get_starved_table(start,end)
    # cycle_time_table = get_cycle_time_table(start,end)
    performance_table = get_performance_table(start,end)
    mamc_yield_table = get_mamc_yield_table(start,end)
    c3a_yield_table = get_c3a_yield_table(start,end)
    cycle_time_html = '<table>' + "<caption>Performance</caption>"  + performance_table + '</table>'
    z2_yield_html = '<table>' + "<caption>Yield</caption>" + header_html + mamc_yield_table + c3a_yield_table + '</table>'

    #get webhook based on environment
    webhook_key = 'teams_webhook_BMA8_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']

    #start end of shift message
    teams_msg = pymsteams.connectorcard(webhook)
    title = 'BMA8 ZONE2 EOS Report' if eos else 'BMA8 ZONE2 Hourly Update'
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

    cycle_card = pymsteams.cardsection()
    cycle_card.text(cycle_time_html)
    yield_card = pymsteams.cardsection()
    yield_card.text(z2_yield_html)

    teams_msg.addSection(cycle_card)
    teams_msg.addSection(yield_card)

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
