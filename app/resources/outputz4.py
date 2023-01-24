from common import helper_functions

from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import pymsteams
import traceback


def get_starved_table(db, start, end):
    pi_paths = [
        '[MC1_Zone1]_OEE_Reporting/TSMs/MC1_01000_01_Module Load_R01',
        '[MC2_10000_01]_OEE_Reporting/TSMs/MC2_10000_01_ST010_MDL10_Robot'
    ]
    po_paths = [
        '[MC1_Zone6]_OEE_Reporting/TSMs/MC1_30000_01_Robot26',
        '[MC2_28000_01]_OEE_Reporting/TSMs/MC2_28000_01_ST250_MDL10_Robot'
    ]

    seconds_between = (end - start).seconds

    pi_df = helper_functions.query_tsm_state(db, start, end, pi_paths, 'Starved', 1)
    po_df = helper_functions.query_tsm_state(db, start, end, po_paths, 'Starved', 2)

    pi1_starved = round(helper_functions.get_val(pi_df, 'MC1-', 'LINE', 'Duration') / seconds_between * 100, 1)
    pi2_starved = round(helper_functions.get_val(pi_df, 'MC2-', 'LINE', 'Duration') / seconds_between * 100, 1)

    po1_starved = round(helper_functions.get_val(po_df, 'MC1-', 'LINE', 'Duration') / seconds_between * 100, 1)
    po2_starved = round(helper_functions.get_val(po_df, 'MC2-', 'LINE', 'Duration') / seconds_between * 100, 1)

    html = f"""
        <tr>
            <td></td>
            <th style="text-align:center"><strong>MC1</strong></th>
            <th style="text-align:center"><strong>MC2</strong></th>
        </tr>
        <tr>
            <td style="text-align:left"><b>Pack-in</b></td>
            <td style="text-align:center">{pi1_starved}%</td>
            <td style="text-align:center">{pi2_starved}%</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>Pack-out</b></td>
            <td style="text-align:center">{po1_starved}%</td>
            <td style="text-align:center">{po2_starved}%</td>
        </tr>
        """
    return pi1_starved, pi2_starved, po1_starved, po2_starved


def get_pallet_count_MC1(pr_db, flow_steps, time_frame=12):
    query = f"""
                SELECT A.PalletType as 'Pallet Type', count(distinct A.PALLET_ID) as 'Pallet Count'
            FROM(
                    Select 
                    CASE when PALLET_ID like 'NIC%' then 'NIC'
                         when PALLET_ID like 'IC%' then 'A2_IC'
                         when PALLET_ID like '1%' then 'IC' END as PalletType,
                    PALLET_ID, convert_Tz(max(LAST_REQUEST_TIME) ,'UTC','US/Pacific') as 'LastTime'
                    From pallet_history
                    Where (PALLET_ID LIKE "NIC%" or PALLET_ID LIKE "IC%" or PALLET_ID LIKE "1%") AND 
                          DESTINATION in ({flow_steps})
                          AND LAST_REQUEST_TIME > DATE_SUB(now(), INTERVAL {time_frame} HOUR)
                    Group by PALLET_ID) as A
            WHERE A.LastTime  > ( SELECT convert_Tz(max(LAST_REQUEST_TIME) ,'UTC','US/Pacific') - interval 1 hour as 'LastTime'
                                  FROM pallet_history
                                  WHERE DESTINATION LIKE "MC1%" AND 
                                  (PALLET_ID LIKE "NIC%" or PALLET_ID LIKE "IC%" or PALLET_ID LIKE "1%")
                                        AND LAST_REQUEST_TIME > DATE_SUB(now(), INTERVAL {time_frame} HOUR))
            GROUP BY A.PalletType
        """
    try:
        df = pd.read_sql(query, pr_db)
        NIC = df.iloc[2][1]
        IC = df.iloc[1][1]
        A2 = df.iloc[0][1]
    except Exception:
        logging.error(traceback.print_exc())

    return NIC, IC, A2


def get_pallet_count_MC2(pr_db, mos_db, flow_steps, pallet_type=0, time_frame=12):
    pallet = ''
    if pallet_type == 0:
        pallet = 'NIC'
    elif pallet_type == 1:
        pallet = 'IC'

    query1 = f"""
                    SELECT distinct A.PALLET_ID, B.SERIAL_NUMBER
                    FROM
                        (SELECT PALLET_ID, convert_Tz(max(LAST_REQUEST_TIME) ,'UTC','US/Pacific') as 'LastTime'
                        FROM pallet_history
                        WHERE DESTINATION LIKE "MC2%" AND PALLET_ID LIKE "{pallet}%"
                             AND LAST_REQUEST_TIME > DATE_SUB(now(), INTERVAL {time_frame} HOUR)
                        GROUP BY PALLET_ID
                        ORDER BY 'Last Time') as A
                        INNER JOIN
                        (SELECT PALLET_ID, SERIAL_NUMBER, DESTINATION, 
                        convert_Tz(max(LAST_REQUEST_TIME) ,'UTC','US/Pacific') as 'LastTime'
                        FROM pallet_history
                        WHERE DESTINATION LIKE "MC2%" AND PALLET_ID LIKE "{pallet}%"
                             AND LAST_REQUEST_TIME > DATE_SUB(now(), INTERVAL {time_frame} HOUR)
                        GROUP BY PALLET_ID, SERIAL_NUMBER, DESTINATION
                        ORDER BY 'Last Time') as B ON A.PALLET_ID = B.PALLET_ID AND A.LastTime = B.LastTime
                    WHERE B.DESTINATION in ({flow_steps}) 
                    AND A.LastTime > ( Select convert_Tz(max(LAST_REQUEST_TIME) ,'UTC','US/Pacific') - interval 0.5 hour as 'LastTime'
                                           FROM pallet_history
                                            WHERE DESTINATION LIKE "MC2%" AND PALLET_ID LIKE "NIC%"
                                            AND LAST_REQUEST_TIME > DATE_SUB(now(), INTERVAL {time_frame} HOUR))
                    ORDER BY PALLET_ID
                """
    try:
        data = pd.read_sql(query1, pr_db)
        data = data.values.tolist()

        serial_data = [x[1] for x in data]
        serials = f""
        for serial in serial_data:
            serials = serials + f"'{serial}',"
        serials = serials[:-1]
    except Exception:
        logging.error(traceback.print_exc())

    query2 = f"""
                    SELECT A.ModType, count(*) as Pallet_Count
                    FROM (Select name, 
                            CASE when left(mid(description, 12,14),1) = 1 OR left(mid(description, 12,14),1) = 4 THEN '23s'
                                 when left(mid(description, 12,14),1) = 2 OR left(mid(description, 12,14),1) = 3 THEN '25s' 
                                 END as ModType
                            From thing
                            where name in ({serials})) as A
                    GROUP BY ModType
                    ORDER BY A.ModType"""
    try:
        df = pd.read_sql(query2, mos_db)
        mod23s = df.iloc[0][1]
        mod25s = df.iloc[1][1]
        return mod23s, mod25s
    except Exception:
        logging.error(traceback.print_exc())

def format_flow_steps(flow_steps):
    valid_flow_steps = f""
    for flow_step in flow_steps:
        valid_flow_steps = valid_flow_steps + f"'{flow_step}',"
    valid_flow_steps = valid_flow_steps[:-1]

    return valid_flow_steps

def get_pallet_color_reporting(value, line, pallet_type):
    limits = [0, 0]

    if line == 'MC1':
        if pallet_type == 'NIC':
            limits = [110, 100]
        elif pallet_type == 'IC':
            limits = [60, 50]
        elif pallet_type == 'A2':
            limits = [15, 11]
    elif line == 'MC2':
        if pallet_type == 'NIC':
            limits = [46, 40]
        elif pallet_type == 'IC':
            limits = [28, 22]

    HTML_style = ["bgcolor=#28a745 style='text-align:center;color:white'", "bgcolor=#ffc107 style='text-align:center'",
                  "bgcolor=#dc3545 style='text-align:center;color:white'"]

    if value >= limits[0]:
        style = HTML_style[0]
    elif value >= limits[1]:
        style = HTML_style[1]
    else:
        style = HTML_style[2]

    return style


def main(env, eos=False):
    logging.info("Z4 start %s" % datetime.utcnow())

    lookback = 12 if eos else 1
    now = datetime.utcnow()
    now_sub1hr = now + timedelta(hours=-lookback)
    start = now_sub1hr.replace(minute=00, second=00, microsecond=00)
    end = start + timedelta(hours=lookback)

    MC1_FLOWSTEP = 'MC1-30000'
    MC2_FLOWSTEP = 'MC2-28000'

    flowsteps = [MC1_FLOWSTEP, MC2_FLOWSTEP]

    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    plc_con = helper_functions.get_sql_conn('plc_db')
    pr_con = helper_functions.get_sql_conn('gf1_pallet_management', 'gf1_pallet_management')  # pallet record data

    df_output = helper_functions.get_flowstep_outputs(mos_con, start, end, flowsteps)
    mc1_output = helper_functions.get_output_val(df_output, MC1_FLOWSTEP)
    mc2_output = helper_functions.get_output_val(df_output, MC2_FLOWSTEP)
    mic_total = mc1_output + mc2_output

    pi1_starved, pi2_starved, po1_starved, po2_starved = get_starved_table(plc_con, start, end)  # pull starvation data

    # ======================================================================================================================
    MC1_flow_steps = ['MC1-01350', 'MC1-02000', 'MC1-02200', 'MC1-02500', 'MC1-02600', 'MC1-03000', 'MC1-04000',
                      'MC1-06000', 'MC1-06500', 'MC1-07000', 'MC1-08000', 'MC1-09000', 'MC1-10000', 'MC1-12000',
                      'MC1-14000', 'MC1-15250', 'MC1-15500', 'MC1-16000', 'MC1-17000', 'MC1-19000', 'MC1-21250',
                      'MC1-03000', 'MC1-03200', 'MC1-04000', 'MC1-21250', 'MC1-21500', 'MC1-21900', 'MC1-23000',
                      'MC1-24000', 'MC1-26000', 'MC1-29000']
    MC1_flow_steps = format_flow_steps(MC1_flow_steps)
    MC1_NIC, MC1_IC, MC1_A2 = get_pallet_count_MC1(pr_con, MC1_flow_steps)
    MC1_NIC_style = get_pallet_color_reporting(MC1_NIC, 'MC1', 'NIC')
    MC1_IC_style = get_pallet_color_reporting(MC1_IC, 'MC1', 'IC')
    MC1_A2_style = get_pallet_color_reporting(MC1_A2, 'MC1', 'A2')

    MC2_NIC_flow_steps = ['MC2-10000', 'MC2-11000', 'MC2-12000', 'MC2-12500', 'MC2-13000', 'MC2-14000', 'MC2-15000',
                          'MC2-16000', 'MC2-17000', 'MC2-18000', 'MC2-19000', 'MC2-19500', 'MC2-20000']
    MC2_NIC_flow_steps = format_flow_steps(MC2_NIC_flow_steps)
    MC2_NIC23, MC2_NIC25 = get_pallet_count_MC2(pr_con, mos_con, MC2_NIC_flow_steps)
    MC2_NIC23_style = get_pallet_color_reporting(MC2_NIC23, 'MC2', 'NIC')
    MC2_NIC25_style = get_pallet_color_reporting(MC2_NIC25, 'MC2', 'NIC')

    MC2_IC_flow_steps = ['MC2-20000', 'MC2-21000', 'MC2-22000', 'MC2-23000', 'MC2-26000', 'MC2-27000', 'MC2-28000']
    MC2_IC_flow_steps = format_flow_steps(MC2_IC_flow_steps)
    MC2_IC23, MC2_IC25 = get_pallet_count_MC2(pr_con, mos_con, MC2_IC_flow_steps, 1)
    MC2_IC23_style = get_pallet_color_reporting(MC2_IC23, 'MC2', 'IC')
    MC2_IC25_style = get_pallet_color_reporting(MC2_IC25, 'MC2', 'IC')

    pallet_html = f"""
             <table cellpadding="8" border ='1'>
              <tr bgcolor = #f2f2f2>
                <th rowspan="2" style="text-align:center"></th>
                <th bgcolor = #f2f2f2 rowspan="1" style="text-align:center">MC1</th>
                <th bgcolor = #f2f2f2 colspan="2" style="text-align:center">MC2</th>
              </tr>


              <tr bgcolor = #f2f2f2 >
                <th bgcolor = #f2f2f2 rowspan="1" style="text-align:center"></th>
                <th style="text-align:center">23s</th>
                <th style="text-align:center">25s</th>
              </tr>

              <tr>
                <td style="text-align:center"><strong>NIC</strong></td>
                <td {MC1_NIC_style}>{MC1_NIC}</td>
                <td {MC2_NIC23_style}>{MC2_NIC23}</td>
                <td {MC2_NIC25_style}>{MC2_NIC25}</td>
              </tr>

              <tr bgcolor = #f2f2f2>
                <td style="text-align:center"><strong>IC</strong></td>
                <td {MC1_IC_style}>{MC1_IC}</td>
                <td {MC2_IC23_style}>{MC2_IC23}</td>
                <td {MC2_IC25_style}>{MC2_IC25}</td>
              </tr>

              <tr>
                <td style="text-align:right"><strong>Area 2</strong></td>
                <td {MC1_A2_style}>{MC1_A2}</td>
                <td style="text-align:center">-</td>
                <td style="text-align:center">-</td>
              </tr>
            </table>"""
    # ======================================================================================================================

    mos_con.close()
    plc_con.close()
    pr_con.close()

    # Setup teams output table
    title = 'Zone 4 Hourly Update'
    html = f"""<table>
                <tr>
                <th style="text-align:center"></th>
                  <th style="text-align:center">Rate</th>
                  <th colspan="2" style="text-align:center">MTR Starvation</th>
                </tr>
            
            
                <tr bgcolor = #f2f2f2 >
                  <th style="text-align:right">LINE</th>
                  <th style="text-align:center">UPH</th>
                  <th style="text-align:center">Pack-in</th>
                  <th style="text-align:center">Pack-out</th>
                </tr>
              
                <tr>
                 <td style="text-align:right"><strong>MC1</strong></td>
                 <td style="text-align:center">{mc1_output/4:.1f}</td>
                 <td style="text-align:center">{pi1_starved}%</td>
                 <td style="text-align:center">{po1_starved}%</td>
                </tr>
            
                <tr bgcolor = #f2f2f2>
                 <td style="text-align:right"><strong>MC2</strong></td>
                 <td style="text-align:center">{mc2_output/4:.1f}</td>
                 <td style="text-align:center">{pi2_starved}%</td>
                 <td style="text-align:center">{po2_starved}%</td>
                </tr>
            
                <tr>
                 <td style="text-align:right"><strong>TOTAL</strong></td>
                 <td style="text-align:center"><b>{mic_total/4:.1f}</b></td>
                 <td style="text-align:center">-</td>
                 <td style="text-align:center">-</td>
                </tr>
            </table>"""

    # get webhook based on environment
    webhook_key = 'teams_webhook_Zone4_Updates' if env == 'prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']

    # start end of shift message
    teams_msg = pymsteams.connectorcard(webhook)
    title = 'Zone 4 EOS Report' if eos else 'Zone 4 Hourly Update'
    teams_msg.title(title)
    teams_msg.summary('summary')
    K8S_BLUE = '#3970e4'
    TESLA_RED = '#cc0000'
    msg_color = TESLA_RED if eos else K8S_BLUE
    teams_msg.color(msg_color)

    # create cards for each major html
    output_card = pymsteams.cardsection()
    output_card.text(html)
    teams_msg.addSection(output_card)
    # create card for pallet count
    pallet_card = pymsteams.cardsection()
    pallet_card.text(pallet_html)
    teams_msg.addSection(pallet_card)
    # add a link to the confluence page
    teams_msg.addLinkButton("Questions?",
                            "https://confluence.teslamotors.com/display/PRODENG/Battery+Module+Hourly+Update")
    # SEND IT
    teams_msg.send()
