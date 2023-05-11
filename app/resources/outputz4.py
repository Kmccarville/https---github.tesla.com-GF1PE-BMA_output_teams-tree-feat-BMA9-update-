from common import helper_functions

from datetime import datetime
from datetime import timedelta
import logging
from sqlalchemy import text
import pandas as pd
import pymsteams
import requests
from io import StringIO
from requests.auth import HTTPBasicAuth

def get_mc1_pallets(db,lookback):
    percent = '%'
    query = f"""
        SELECT
        DISTINCT(PALLET_ID)
        FROM pallet_record
        WHERE line_id LIKE 'M3BM_MC_v1'
        AND pallet_id NOT LIKE 'IC%'
        AND (destination NOT LIKE '%NCM%' OR destination IS NULL)
        AND last_request_time > DATE_SUB(NOW(), INTERVAL 2 HOUR)
        """
    df = pd.read_sql(text(query),db)
    df_nic = df.loc[df['PALLET_ID'].str.contains('NIC')]
    nic = len(df_nic)
    ic = len(df) - nic
    return nic,ic

def get_mc2_pallets(db,tagpath):
    query = f"""
            SELECT sqlth84.intvalue
            FROM rno_ia_taghistory_batterymodule.sqlth_84_data sqlth84
            LEFT JOIN rno_ia_taghistory_batterymodule.sqlth_te te ON sqlth84.tagid = te.id
            LEFT JOIN rno_ia_taghistory_batterymodule.sqlth_scinfo sc ON te.scid = sc.id
            LEFT JOIN rno_ia_taghistory_batterymodule.sqlth_drv drv ON sc.drvid = drv.id
            WHERE
                te.tagpath = '{tagpath}'
            ORDER BY t_stamp DESC
            LIMIT 1
            """
    df = pd.read_sql(query,db)
    count = df.get_value(0,'intvalue')
    return count


def get_starved_table(db, start, end):
    pi_paths = [
        '[MC1_Zone1]_OEE_Reporting/TSMs/MC1_01000_01_Module Load_R01',
        '[MC2_10000_01]_OEE_Reporting/TSMs/MC2_10000_01_ST010_MDL10_Robot'
    ]
    po_paths = [
        '[MC1_Zone6]_OEE_Reporting/TSMs/MC1_30000_01_Robot26',
        '[MC2_28000_01]_OEE_Reporting/TSMs/MC2_28000_01_ST250_MDL10_Robot'
    ]

    mc2_pi_path = '[MC2_10000_01]_OEE_Reporting/TSMs/MC2_10000_01_ST010_MDL10_Robot'

    seconds_between = (end - start).seconds

    pi_df = helper_functions.query_tsm_state(db, start, end, pi_paths, 'Starved', 1)
    po_df = helper_functions.query_tsm_state(db, start, end, po_paths, 'Starved', 2)
    mc2_po_df5 = helper_functions.query_tsm_state(db, start, end, [mc2_pi_path], 'Blocked', 5) #starved for 25s
    mc2_po_df6 = helper_functions.query_tsm_state(db, start, end, [mc2_pi_path], 'Blocked', 6) #starved for 23s

    pi1_starved = round(helper_functions.get_val(pi_df, 'MC1-', 'LINE', 'Duration') / seconds_between * 100, 1)
    pi2_starved = round(helper_functions.get_val(pi_df, 'MC2-', 'LINE', 'Duration') / seconds_between * 100, 1)

    po1_starved = round(helper_functions.get_val(po_df, 'MC1-', 'LINE', 'Duration') / seconds_between * 100, 1)
    po2_starved = round(helper_functions.get_val(po_df, 'MC2-', 'LINE', 'Duration') / seconds_between * 100, 1)

    pi2_starved5 = round(helper_functions.get_val(mc2_po_df5, 'MC2-', 'LINE', 'Duration') / seconds_between * 100, 1)
    pi2_starved6 = round(helper_functions.get_val(mc2_po_df6, 'MC2-', 'LINE', 'Duration') / seconds_between * 100, 1)


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
        <tr>
            <td style="text-align:left"><b>No 23S</b></td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">{pi2_starved6}%</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>No 25S</b></td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">{pi2_starved5}%</td>
        </tr>
        """
    return html

def getDirFeedData(MC1_UPH, MC2_UPH): 
    #...................................................#
    # function take MC1 & MC2 hrly UPH                  #
    # Returns object with MTR supplied in DF & Rate (%) #
    #...................................................#

    # initiate params
    start = datetime.now() 
    end = start - timedelta(hours=1) 
    start = str(start) 
    end = str(end) 
    splunk_start = start.replace(' ','T') + '.000-07:00' # can use start and end to run splnk query on specific time frame
    splunk_end = end.replace(' ','T') + '.000-07:00' 
    
    creds = helper_functions.get_pw_json("sa_splunk")
    user = creds['user']
    pw = creds['password']
    MC1_query = f"""search index=mes sourcetype="ignition:custom:json:log" logger_name="GFNV_MC1_DirectFeed" | fields- _raw | spath input=message | where DirectFeedAction="True" | stats c(container) as container""" 
    MC2_query = f"""search index=mes sourcetype="ignition:custom:json:log" logger_name="GFNV_MC2_DirectFeed" | fields- _raw | spath input=message | where directfeed="True" | stats c(container) as container""" 
    
    Query = [MC1_query,MC2_query] 
    DF_Data = [] 
    return_obj = {} 
    
    for query in range(len(Query)): 
        
        response = requests.get( 
                                url='https://splunkapi.teslamotors.com/services/search/jobs/export', 
                                auth= HTTPBasicAuth(user, pw), 
                                params={ 'search': Query[query], 
                                        'adhoc_search_level': 'fast',  # [ verbose | fast | smart ] 
                                        'auto_cancel': 0, # If specified, the job automatically cancels after this many seconds of inactivity. (0 means never auto-cancel)
                                        'earliest_time': '-1h', 
                                        #'latest_time': splunk_end,
                                        'output_mode': 'csv' # (atom | csv | json | json_cols | json_rows | raw | xml) 
                                        }
                                ) 
        if response.text: 
            splunk_text = StringIO(response.text) # convert csv string into a StringIO to be processed by pandas 
            df = pd.read_csv(splunk_text) 
            directfeed = df['container'][0] #splunk search return only one value 
            DF_Data.append(directfeed) 
        else: 
            DF_Data.append(-1) 
        
        MC1_DF = int(DF_Data[0])
        MC2_DF = int(DF_Data[1]) 
        
        return_obj['MC1_DF'] = MC1_DF 
        return_obj['MC2_DF'] = MC2_DF
        return_obj['Total_DF'] = MC1_DF + MC2_DF 
        return_obj['MC1_DF_Rate'] = round((MC1_DF/MC1_UPH)*100,1) 
        return_obj['MC2_DF_Rate'] = round((MC2_DF/MC2_UPH)*100,1) 
        return_obj['Total_DF_Rate'] = round(((MC1_DF + MC2_DF) / (MC1_UPH + MC2_UPH))*100,1) 
        
        return return_obj

def main(env, eos=False):
    logging.info("Z4 start %s" % datetime.utcnow())

    lookback = 12 if eos else 1
    now = datetime.utcnow()
    now_sub1hr = now + timedelta(hours=-lookback)
    start = now_sub1hr.replace(minute=00, second=00, microsecond=00)
    end = start + timedelta(hours=lookback)

    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    pr_con = helper_functions.get_sql_conn('gf1_pallet_management',schema='gf1_pallet_management')
    plc_con = helper_functions.get_sql_conn('plc_db')


    MC1_FLOWSTEP = 'MC1-30000'
    MC2_FLOWSTEP = 'MC2-28000'
    flowsteps = [MC1_FLOWSTEP, MC2_FLOWSTEP]
    df_output = helper_functions.get_flowstep_outputs(mos_con, start, end, flowsteps)
    mc1_output = helper_functions.get_output_val(df_output, MC1_FLOWSTEP)
    mc2_output = helper_functions.get_output_val(df_output, MC2_FLOWSTEP)
    mic_total = mc1_output + mc2_output

    directfeed = getDirFeedData(mc1_output/4,mc2_output/4)
    mc1_df_count = directfeed['MC1_DF']
    mc2_df_count = directfeed['MC2_DF']
    mc1_df_rate = directfeed['MC1_DF_Rate']
    mc2_df_rate = directfeed['MC2_DF_Rate']    
    mc_total_df_count = directfeed['Total_DF']
    mc_total_df_rate = directfeed['Total_DF_Rate']

    # setup query constants
    MC1_PALLET_LOOKBACK = 2 #HOURS
    
    MC2_NIC1_TAGPATH = 'nic lanes/stscountlane4'
    MC2_NIC2_TAGPATH = 'nic lanes/stscountlane1'
    MC2_NIC3_TAGPATH = 'nic lanes/stscountlane2'
    MC2_NIC4_TAGPATH = 'nic lanes/stscountlane3'
    MC2_IC14_TAGPATH = 'TotalNumberCarriersLane1_4'
    MC2_IC23_TAGPATH = 'TotalNumberCarriersLane2_3'

    
    #setup threshold constants
    MC1_NIC_GREEN = 110
    MC1_NIC_YELLOW = 100
    MC1_IC_GREEN = 60
    MC1_IC_YELLOW = 50

    MC2_NIC_GREEN = 50
    MC2_NIC_YELLOW = 43
    MC2_IC_GREEN = 28
    MC2_IC_YELLOW = 25


    mc1_nic_pallets,mc1_ic_pallets = get_mc1_pallets(pr_con, MC1_PALLET_LOOKBACK)

    mc2_nic1_pallets = get_mc2_pallets(plc_con , MC2_NIC1_TAGPATH)
    mc2_nic2_pallets = get_mc2_pallets(plc_con , MC2_NIC2_TAGPATH)
    mc2_nic3_pallets = get_mc2_pallets(plc_con , MC2_NIC3_TAGPATH)
    mc2_nic4_pallets = get_mc2_pallets(plc_con , MC2_NIC4_TAGPATH)
    mc2_nic14_pallets = mc2_nic1_pallets + mc2_nic4_pallets
    mc2_nic23_pallets = mc2_nic2_pallets + mc2_nic3_pallets
    mc2_ic14_pallets = get_mc2_pallets(plc_con , MC2_IC14_TAGPATH)
    mc2_ic23_pallets = get_mc2_pallets(plc_con , MC2_IC23_TAGPATH)

    if mc1_nic_pallets >= MC1_NIC_GREEN:
        mc1_nic_color = 'green'
    elif mc1_nic_pallets >= MC1_NIC_YELLOW:
        mc1_nic_color = 'orange'
    else: mc1_nic_color = 'red'

    if mc1_ic_pallets >= MC1_IC_GREEN:
        mc1_ic_color = 'green'
    elif  mc1_ic_pallets >= MC1_IC_YELLOW:
        mc1_ic_color = 'orange'
    else: mc1_ic_color = 'red'

    if mc2_nic14_pallets >= MC2_NIC_GREEN:
        mc2_nic14_color = 'green'
    # elif mc2_nic14_pallets >= MC2_NIC_YELLOW:
    #    mc2_nic14_color = 'orange'
    else:
        mc2_nic14_color = 'red'

    if mc2_nic23_pallets >= MC2_NIC_GREEN:
        mc2_nic23_color = 'green'
    # elif mc2_nic23_pallets >= MC2_NIC_YELLOW:
    #    mc2_nic23_color = 'orange'
    else:
        mc2_nic23_color = 'red'

    if mc2_ic14_pallets >= MC2_IC_GREEN:
        mc2_ic14_color = 'green'
    # elif mc2_ic14_pallets >= MC2_IC_YELLOW:
    #    mc2_ic14_color = 'orange'
    else:
        mc2_ic14_color = 'red'

    if mc2_ic23_pallets >= MC2_IC_GREEN:
        mc2_ic23_color = 'green'
    # elif mc2_ic23_pallets >= MC2_IC_YELLOW:
    #    mc2_ic23_color = 'orange'
    else: 
        mc2_ic23_color = 'red'

    starve_table = get_starved_table(plc_con, start, end)  # pull starvation data

    mos_con.close()
    plc_con.close()
    pr_con.close()

    # Setup teams output table
    title = 'Zone 4 Hourly Update'
    html = f"""<table>
            <tr>
                <th style="text-align:right"></th>
                <th style="text-align:center">UPH</th>
                <th style="text-align:center">DF Count</th>
                <th style="text-align:center">DF Rate</th>
            </tr>
            <tr>
                <td style="text-align:right"><strong>MC1</strong></td>
                <td <td style="text-align:left">{mc1_output/4:.1f}</td>
                <td <td style="text-align:left">{mc1_df_count:.1f}</td>
                <td <td style="text-align:left">{mc1_df_rate:.1f}</td>
            </tr>
            <tr>
                <td style="text-align:right"><strong>MC2</strong></td>
                <td style="text-align:left">{mc2_output/4:.1f}</td>
                <td <td style="text-align:left">{mc2_df_count:.1f}</td>
                <td <td style="text-align:left">{mc2_df_rate:.1f}</td>
            </tr>
            <tr>
                <td style="text-align:right"><strong>TOTAL</strong></td>
                <td style="text-align:left"><b>{mic_total/4:.1f}</b></td>
                <td <td style="text-align:left">{mc_total_df_count:.1f}</td>
                <td <td style="text-align:left">{mc_total_df_rate:.1f}</td>
            </tr>
            </table>"""
    na_html = "---"
    pallet_html = f"""
                <tr>
                    <th style="text-align:right"></th>
                    <th style="text-align:center">NIC</th>
                    <th style="text-align:center">IC</th>
                    <th style="text-align:center">NIC 1_4</th>
                    <th style="text-align:center">NIC 2_3</th>
                    <th style="text-align:center">IC 1_4</th>
                    <th style="text-align:center">IC 2_3</th>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>MC1</strong></td>
                    <td <td style="text-align:center;color:{mc1_nic_color}">{mc1_nic_pallets}</td>
                    <td <td style="text-align:center;color:{mc1_ic_color}">{mc1_ic_pallets}</td>
                    <td <td style="text-align:center">{na_html}</td>
                    <td <td style="text-align:center">{na_html}</td>
                    <td <td style="text-align:center">{na_html}</td>
                    <td <td style="text-align:center">{na_html}</td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>MC2</strong></td>
                    <td <td style="text-align:center">{na_html}</td>
                    <td <td style="text-align:center">{na_html}</td>
                    <td <td style="text-align:center;color:{mc2_nic14_color}">{mc2_nic14_pallets} ({mc2_nic1_pallets}+{mc2_nic4_pallets})</td>
                    <td <td style="text-align:center;color:{mc2_nic23_color}">{mc2_nic23_pallets} ({mc2_nic2_pallets}+{mc2_nic3_pallets})</td>
                    <td <td style="text-align:center;color:{mc2_ic14_color}">{mc2_ic14_pallets}</td>
                    <td <td style="text-align:center;color:{mc2_ic23_color}">{mc2_ic23_pallets}</td>
                </tr>
                <tr>
                    <td style="text-align:right"><strong>GOAL</strong></td>
                    <td <td style="text-align:center">{MC1_NIC_GREEN}</td>
                    <td <td style="text-align:center">{MC1_IC_GREEN}</td>
                    <td <td style="text-align:center">{MC2_NIC_GREEN}</td>
                    <td <td style="text-align:center">{MC2_NIC_GREEN}</td>
                    <td <td style="text-align:center">{MC2_IC_GREEN}</td>
                    <td <td style="text-align:center">{MC2_IC_GREEN}</td>
                </tr>
                """
    pallet_html = "<table>" + "<caption><u>Pallet Count</u></caption>" + pallet_html + "</table>"
    # Setup teams starvation table
    starved_html = "<table>" + "<caption><u>MTR Starvation</u></caption>" + starve_table + "</table>"

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
    # create cards for pallet counts
    pallet_card = pymsteams.cardsection()
    pallet_card.text(pallet_html)
    teams_msg.addSection(pallet_card)
    # make a card with starvation data
    starved_card = pymsteams.cardsection()
    starved_card.text(starved_html)
    teams_msg.addSection(starved_card)
    # add a link to the confluence page
    teams_msg.addLinkButton("Questions?",
                            "https://confluence.teslamotors.com/display/PRODENG/Battery+Module+Hourly+Update")
    #SEND IT
    try:
        teams_msg.send()
    except TimeoutError:
        logging.info("Webhook timed out, retry once")
        try:
            teams_msg.send()
        except TimeoutError:
            logging.info("Webhook timeded out twice -- pass to next area")
            pass

