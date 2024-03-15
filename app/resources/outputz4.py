import logging
import traceback
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import pymsteams
import pytz
import requests
from common import helper_functions
from common.constants import K8S_BLUE, TESLA_RED, Z4_DIVISOR
import pytz
from requests.auth import HTTPBasicAuth
from sqlalchemy import text


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
    return html, pi1_starved, pi2_starved, po1_starved, po2_starved, pi2_starved6, pi2_starved5

def get_fpy_dfs(db, lookback, line='MC1'):
    line_val = '908336' if line == 'MC1' else '906294'
    
    query = f"""
        SELECT
        prod.Process,
        prod.Shift_Day,
        prod.Shift,
        prod.Total_Production,
        prod.Total_Production - ifnull(nc.NC_Count, 0) as Good_Mods,
        ifnull(nc.NC_Count, 0) as Defect_Mods,
        (prod.Total_Production - ifnull(nc.NC_Count, 0)) / prod.Total_Production as FPY


        FROM

        (SELECT
        --  tp.flowstepid,
        left(tp.flowstepname, 3) as Process,
        date_format(convert_tz(tp.completed, 'UTC', 'US/Pacific') - interval 6 hour, "%Y-%m/%d")   as Shift_Day,  ### This is to force night shifts onto 1 calendar day ###
        CASE FLOOR((UNIX_TIMESTAMP(CONVERT_TZ(tp.completed,'UTC','US/Pacific'))-1452405600) % 1209600 / 43200 )   ### 1209600 = seconds in 2 weeks and 43200 = seconds in 12 hours ###
                WHEN 0 THEN 'A'
                WHEN 1 THEN 'C'
                WHEN 2 THEN 'A'
                WHEN 3 THEN 'C'
                WHEN 4 THEN 'A'
                WHEN 5 THEN 'C'
                WHEN 6 THEN 'A'
                WHEN 7 THEN 'C'
                WHEN 8 THEN 'B'
                WHEN 9 THEN 'D'
                WHEN 10 THEN 'B'
                WHEN 11 THEN 'D'
                WHEN 12 THEN 'B'
                WHEN 13 THEN 'D'
                WHEN 14 THEN 'A'
                WHEN 15 THEN 'C'
                WHEN 16 THEN 'A'
                WHEN 17 THEN 'C'
                WHEN 18 THEN 'A'
                WHEN 19 THEN 'C'
                WHEN 20 THEN 'B'
                WHEN 21 THEN 'D'
                WHEN 22 THEN 'B'
                WHEN 23 THEN 'D'
                WHEN 24 THEN 'B'
                WHEN 25 THEN 'D'
                WHEN 26 THEN 'B'
                WHEN 27 THEN 'D'
            END   AS 'Shift',
        count(distinct(tp.thingid)) as Total_Production
            
            from thingpath tp
            where tp.flowstepid in ('{line_val}')   ### MC1 and MC2 ###
            and tp.completed > now() - interval {lookback} hour
            and tp.iscurrent = 0
            
            group by 1,2,3
            )prod
            
        left join
        
        (select
        nc.detectedatprocess as Process,
        date_format(convert_tz(nc.created, 'UTC', 'US/Pacific') - interval 6 hour, "%Y-%m/%d")   as Shift_Day,  ### This is to force night shifts onto 1 calendar day ###
        CASE FLOOR((UNIX_TIMESTAMP(CONVERT_TZ(nc.created,'UTC','US/Pacific'))-1452405600) % 1209600 / 43200 )   ### 1209600 = seconds in 2 weeks and 43200 = seconds in 12 hours ###
                WHEN 0 THEN 'A'
                WHEN 1 THEN 'C'
                WHEN 2 THEN 'A'
                WHEN 3 THEN 'C'
                WHEN 4 THEN 'A'
                WHEN 5 THEN 'C'
                WHEN 6 THEN 'A'
                WHEN 7 THEN 'C'
                WHEN 8 THEN 'B'
                WHEN 9 THEN 'D'
                WHEN 10 THEN 'B'
                WHEN 11 THEN 'D'
                WHEN 12 THEN 'B'
                WHEN 13 THEN 'D'
                WHEN 14 THEN 'A'
                WHEN 15 THEN 'C'
                WHEN 16 THEN 'A'
                WHEN 17 THEN 'C'
                WHEN 18 THEN 'A'
                WHEN 19 THEN 'C'
                WHEN 20 THEN 'B'
                WHEN 21 THEN 'D'
                WHEN 22 THEN 'B'
                WHEN 23 THEN 'D'
                WHEN 24 THEN 'B'
                WHEN 25 THEN 'D'
                WHEN 26 THEN 'B'
                WHEN 27 THEN 'D'
            END   as Shift,
        count(distinct(nc.thingid)) as NC_Count
            
        from nc
        
        where nc.detectedatprocess in ('{line}')
        and nc.created > now() - interval {lookback} HOUR

        group by 1,2,3
        ) as nc on nc.Process = prod.Process and nc.Shift_Day = prod.Shift_Day and nc.Shift = prod.Shift

        group by 1,2,3,4,5,6,7
        order by prod.Shift_Day asc,
                        prod.Shift asc   
    """
    
    # combine dataframes here from mc1 df and mc2 df
    df = pd.read_sql(text(query), db)
    return df

def get_fpy(db, lookback):
    mc1_line = get_fpy_dfs(db, lookback, 'MC1')
    mc2_line = get_fpy_dfs(db, lookback, 'MC2')
    #df = pd.concat([mc1_line, mc2_line], ignore_index=True, axis=0)
    return mc1_line, mc2_line

def getDirFeedData(line, uph, eos): 
    #...................................................#
    # function take MC1 & MC2 hrly UPH                  #
    # Returns object with MTR supplied in DF & Rate (%) #
    #...................................................#
    lookback = 12 if eos else 1
  
    #eval start & end tstamp for query
    now = datetime.now(tz=pytz.utc)
    now = now.astimezone(pytz.timezone('US/Pacific'))
    now_sub1hr = now + timedelta(hours = -lookback)
    
    start = now_sub1hr.replace(minute = 00, second = 00, microsecond = 00)
    end = start + timedelta(hours = lookback)
    splunk_start = str(start)
    splunk_end = str(end)
    splunk_start = splunk_start.replace(' ','T') #+ '.000-07:00'
    splunk_end = splunk_end.replace(' ','T') #+ '.000-07:00'

    creds = helper_functions.get_pw_json("sa_splunk")
    user = creds['user']
    pw = creds['password']
    mc1_query = f"""search index=mes sourcetype="ignition:custom:json:log" logger_name="GFNV_MC1_DirectFeed" | fields- _raw | spath input=message | where DirectFeedAction="True" | stats c(container) as container"""
    mc2_query = f"""search index=mes sourcetype="ignition:custom:json:log" logger_name="GFNV_MC2_DirectFeed" |spath input=message |eval df_reason=case(directfeed="True","SUCCESS",Kitmatch="False", "BAD_KIT",mtr_qty_good=="true" AND proj_delta_good=="false" AND type_good=="false", "WRONG_PART", mtr_qty_good=="false", "MC2_FULL") |stats count(eval(directfeed="True")) as SUCCESS, count(eval(Kitmatch="False")) as BAD_KIT, count(eval(mtr_qty_good=="true" AND proj_delta_good=="false" AND type_good=="false")) as WRONG_PART, count(eval(mtr_qty_good=="false")) as MC2_FULL, BY partnumber"""
    df_performance = None
    if line == 'MC1': 
        query = mc1_query 
    else: 
        query = mc2_query
    
    return_obj = {}
    response = requests.get( 
                            url='https://splunkapi.teslamotors.com/services/search/jobs/export', 
                            auth=   HTTPBasicAuth(user, pw),
                            params={ 
                                    'search': query, 
                                    'adhoc_search_level': 'fast', # [ verbose | fast | smart ] 
                                    'auto_cancel': 0, # If specified, the job automatically cancels after this many seconds of inactivity. (0 means never auto-cancel) 
                                    'earliest_time': splunk_start, 
                                    'latest_time': splunk_end, 
                                    'output_mode': 'csv' # (atom | csv | json | json_cols | json_rows | raw | xml) 
                                    } 
                            ) 
    if response.text: 
        splunk_text = StringIO(response.text) # convert csv string into a StringIO to be processed by pandas 
        df = pd.read_csv(splunk_text)
        if line == 'MC1':
            directfeed = int(df['container'][0])
            bad_part = 0
            not_ready = 0
            bad_kit = 0
        else:
            directfeed = int(df['SUCCESS'].sum())
            bad_part = int(df['WRONG_PART'].sum())
            bad_kit = int(df['BAD_KIT'].sum())
            not_ready = int(df['MC2_FULL'].sum())
            try:
                df_performance = round((directfeed/(directfeed + bad_kit + bad_part + not_ready)) * 100 ,0)
            except:
                df_performance = 0
                
        if line == 'MC2':
            try:    
                ict_con = helper_functions.get_sql_conn('interconnect_eng')
                part_list = df['partnumber'].to_list()
                suffix_map = {}
                bad_kits_map = {}
                bad_parts_map = {}
                for part in part_list:
                    part_bad_kits = df.loc[(df['partnumber'] == part), 'BAD_KIT'].sum()
                    part_wrong_parts = df.loc[(df['partnumber'] == part), 'WRONG_PART'].sum()
                    if part_bad_kits < 1 and part_wrong_parts < 1:
                        continue
                    query = f"""
                        SELECT MODEL_SUFFIX FROM m3_bm_process_parameters.static_part_number
                        WHERE Z3_ACTIVE=1 AND UUT_PART_NUMBER LIKE '{part}'
                    """
                    suffix_df = pd.read_sql(text(query), ict_con)
                    if suffix_df.shape[0]:
                        idx = suffix_df['MODEL_SUFFIX'].first_valid_index()
                        suffix = suffix_df['MODEL_SUFFIX'].iloc[idx]
                        suffix_map[suffix] = suffix_map.get(suffix, 0) + 1
                        bad_kits_map[(suffix, part)] = bad_kits_map.get((suffix, part), 0) + part_bad_kits
                        bad_parts_map[(suffix, part)] = bad_parts_map.get((suffix, part), 0) + part_wrong_parts
                return_obj['SuffixMap'] = suffix_map 
                return_obj['BadKitsMap'] = bad_kits_map
                return_obj['BadPartsMap'] = bad_parts_map
                ict_con.close()
            except Exception as e:
                return_obj['SuffixMap'] = {}
                return_obj['BadKitsMap'] = {}
                return_obj['BadPartsMap'] = {}
                logging.error(e)
    else:
        logging.info("Z4 Directfeed data pull - Splunk/DB API failed")
    
    return_obj['DirectFeed'] = directfeed
    return_obj['BadKit'] = bad_kit
    return_obj['BadPart'] = bad_part
    return_obj['NotReady'] = not_ready
    
    try:
        return_obj['Rate'] = round((directfeed/(uph/4))*100,0)
    except:
         return_obj['Rate'] = 0   
    return_obj['DF_Performance'] = df_performance
  
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

 
    #direct feed stats - start
    mc1_dirfeed = getDirFeedData('MC1', mc1_output, eos)
    mc1_df_count = mc1_dirfeed['DirectFeed']

    mc2_dirfeed = getDirFeedData('MC2', mc2_output, eos)
    mc2_df_count = mc2_dirfeed['DirectFeed']
    mc2_df_rate = mc2_dirfeed['Rate']
    mc2_df_notready = mc2_dirfeed['NotReady']
    mc2_df_badpart = mc2_dirfeed['BadPart']
    mc2_df_badkit = mc2_dirfeed['BadKit']
    mc2_df_suffix = mc2_dirfeed['SuffixMap']
    mc2_df_bad_kits_map = mc2_dirfeed['BadKitsMap']
    mc2_df_bad_parts_map = mc2_dirfeed['BadPartsMap']
    mc2_df_performance = mc2_dirfeed['DF_Performance']
  
    total_df_count = mc1_df_count + mc2_df_count
    total_mc_uph = (mc1_output + mc2_output)/4
    #total_df_rate = round(((mc1_df_count + mc2_df_count)/(total_mc_uph))*100 ,0)
    #direct feed stats - end

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

    starve_table, mc1_pi, mc2_pi, mc1_po, mc2_po, no23, no25 = get_starved_table(plc_con, start, end)  # pull starvation data
    
    FPY_GOAL = 94
    mc1_fpy = None
    mc2_fpy = None
    mc1_fpy_color = 'red' 
    mc2_fpy_color = 'red'

    # FPY = passing parts / total parts * 100
    mos_con_sparq = helper_functions.get_sql_conn('mos_rpt2', schema='sparq')
    mc1_fpy, mc2_fpy = get_fpy(mos_con_sparq, lookback)     
    if mc1_fpy.shape[0] < 1:
        mc1_fpy = 0
    else:
        mc1_fpy = mc1_fpy.iloc[-1]['FPY'] * 100
    if mc2_fpy.shape[0] < 1:
        mc2_fpy = 0
    else:
        mc2_fpy = mc2_fpy.iloc[-1]['FPY'] * 100
    
    if mc1_fpy > FPY_GOAL:
        mc1_fpy_color = 'green'
    
    if mc2_fpy > FPY_GOAL:
        mc2_fpy_color = 'green'
        
    mos_con_sparq.close() 
    mos_con.close()
    plc_con.close()
    pr_con.close()

    hourly_goal_dict = helper_functions.get_zone_line_goals(zone=4,hours=lookback)
    # Setup teams output table
    html = f"""<table>
            <tr>
                <th style="text-align:right"></th>
                <th style="text-align:center">UPH</th>
                <th style="text-align:center">UPH Goal</th>
                <th style="text-align:center">FPY (%)</th>
                <th style="text-align:center">FPY Goal (%)</th>
            </tr>
            <tr>
                <td style="text-align:right"><strong>MC1</strong></td>
                <td style="text-align:center">{mc1_output/4:.1f}</td>
                <td style="text-align:center">{int(hourly_goal_dict['MC1'])}</td>
                <td style="text-align:center;color:{mc1_fpy_color}">{mc1_fpy:.2f}</td>
                <td style="text-align:center">{FPY_GOAL:.2f}</td>
            </tr>
            <tr>
                <td style="text-align:right"><strong>MC2</strong></td>
                <td style="text-align:center">{mc2_output/4:.1f}</td>
                <td style="text-align:center">{int(hourly_goal_dict['MC2'])}</td>
                <td style="text-align:center;color:{mc2_fpy_color}">{mc2_fpy:.2f}</td>
                <td style="text-align:center">{FPY_GOAL:.2f}</td>
            </tr>
            <tr>
                <td style="text-align:right"><strong>TOTAL</strong></td>
                <td style="text-align:center"><b>{mic_total/4:.1f}</b></td>
                <td style="text-align:center">---</td>
                <td style="text-align:center">---</td>
                <td style="text-align:center">---</td>
            </tr>
            </table>"""
            
    na_html = "---"
    
    suffix_count_rows = ""
    for suffix in mc2_df_suffix.keys():
        bad_kits_count = sum([val for key, val in mc2_df_bad_kits_map.items() if key[0] == suffix])
        bad_parts_count = sum([val for key, val in mc2_df_bad_parts_map.items() if key[0] == suffix])
        suffix_count_rows += f"""
            <tr>
                <td colspan="2" style="text-align:right">{suffix}</td>
                <td style="text-align:center">{bad_kits_count if bad_kits_count > 0 else '---'}</td>
                <td style="text-align:center">{bad_parts_count if bad_parts_count > 0 else '---'}</td>
            </tr>
        """
        
        
    direct_feed_summary_html = f"""
        <tr>
            <th colspan="2" style="text-align:center">DF Count</th>
            <th style="text-align:center">DF Rate (%)</th>
            <th style="text-align:center">DF Not Ready</th>
        </tr>
        <tr>
            <td colspan="2"  style="text-align:center;color:{'red' if int(mc2_df_count) < 22 else 'green'}">{int(mc2_df_count)}</td>
            <td style="text-align:center">{mc2_df_performance:.1f}</td>
            <td style="text-align:center">{int(mc2_df_notready)}</td>
        </tr>
    """
    
    direct_feed_kit_html = f"""
        <tr>
            <th colspan="2" style="text-align:center"><strong>Suffix</strong></th>
            <th colspan="1" style="text-align:left"><strong>DF Total Bad Kits:</strong> {int(mc2_df_badkit)}</th>
            <th colspan="1" style="text-align:left"><strong>DF Total Bad Parts:</strong> {int(mc2_df_badpart)}</th>
        </tr>
        {suffix_count_rows}
    """
    
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
           
    direct_feed_summary_html = "<table>" + "<caption><u>Direct Feed Breakdown (MC2)</u></caption>" + direct_feed_summary_html + "</table>"
    direct_feed_kit_html = "<table>" + direct_feed_kit_html + "</table>"
    pallet_html = "<table>" + "<caption><u>Pallet Count</u></caption>" + pallet_html + "</table>"
    starved_html = "<table>" + "<caption><u>MTR Starvation</u></caption>" + starve_table + "</table>"

    if env == 'prod':
        teams_con = helper_functions.get_sql_conn('pedb', schema='teams_output')
        try:
            historize_to_db(teams_con, 41, mc1_output/Z4_DIVISOR, mc1_fpy, FPY_GOAL,
                            mc1_nic_pallets, mc1_ic_pallets, None, None, None, None,
                            mc1_pi, mc1_po, None, None,eos)
            historize_to_db(teams_con, 42, mc2_output/Z4_DIVISOR, mc2_fpy, FPY_GOAL,
                            None, None, mc2_nic14_pallets, mc2_nic23_pallets,
                            mc2_ic14_pallets, mc2_ic23_pallets,
                            mc2_pi, mc2_po, no23, no25,eos)
            
        except Exception as e:
            logging.exception(f'Historization for z4 failed. See: {e}')
        teams_con.close()

    # get webhook based on environment

    webhook_key = 'teams_webhook_Zone4_Updates' if env == 'prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']


    # start end of shift message
    teams_msg = pymsteams.connectorcard(webhook)
    title = 'Zone 4 EOS Report' if eos else 'Zone 4 Hourly Update'
    teams_msg.title(title)
    teams_msg.summary('summary')
    msg_color = TESLA_RED if eos else K8S_BLUE
    teams_msg.color(msg_color)

    # create cards for each major html
    output_card = pymsteams.cardsection()
    output_card.text(html)
    teams_msg.addSection(output_card)
    
    direct_feed_summary_card = pymsteams.cardsection()
    direct_feed_summary_card.text(direct_feed_summary_html)
    teams_msg.addSection(direct_feed_summary_card)
    
    direct_feed_kit_card = pymsteams.cardsection()
    direct_feed_kit_card.text(direct_feed_kit_html)
    teams_msg.addSection(direct_feed_kit_card)
    
    pallet_card = pymsteams.cardsection()
    pallet_card.text(pallet_html)
    teams_msg.addSection(pallet_card)

    starved_card = pymsteams.cardsection()
    starved_card.text(starved_html)
    teams_msg.addSection(starved_card)
    
    # add a link to the confluence page
    teams_msg.addLinkButton("Questions?",
                            "https://confluence.teslamotors.com/display/PRODENG/Battery+Module+Hourly+Update")
    #SEND IT
    try:
        teams_msg.send()
    except pymsteams.TeamsWebhookException:
        logging.warn("Webhook timed out, retry once")
        try:
            teams_msg.send()
        except pymsteams.TeamsWebhookException:
            logging.exception("Webhook timed out twice -- pass to next area")
            # helper_functions.e_handler(e)

def historize_to_db(db, _id, uph, fpy, fpy_goal,
                    nic, ic, nic_1_4, nic_2_3, ic_1_4, ic_2_3,
                    pack_in, pack_out, no_23_s, no_25_s,eos):
    sql_date = helper_functions.get_sql_pst_time()    
    df_insert = pd.DataFrame({
        'LINE_ID' : [_id],
        'UPH': [round(uph, 2) if uph is not None else None],
        'FPY_PERCENT': [round(fpy, 2) if fpy is not None else None],
        'FPY_GOAL_PERCENT': [round(fpy_goal, 2) if fpy_goal is not None else None],
        'PALLET_NIC': [nic if nic is not None else None],
        'PALLET_IC': [ic if ic is not None else None],
        'PALLET_NIC_1_4': [nic_1_4 if nic_1_4 is not None else None],
        'PALLET_NIC_2_3': [nic_2_3 if nic_2_3 is not None else None],
        'PALLET_IC_1_4': [ic_1_4 if ic_1_4 is not None else None],
        'PALLET_IC_2_3': [ic_2_3 if ic_2_3 is not None else None],
        'STARVATION_PACK_IN_PERCENT': [pack_in if pack_in is not None else None],
        'STARVATION_PACK_OUT_PERCENT': [pack_out if pack_out is not None else None],
        'STARVATION_NO_23_S_PERCENT': [no_23_s if no_23_s is not None else None],
        'STARVATION_NO_25_S_PERCENT': [no_25_s if no_25_s is not None else None],
        'END_TIME': [sql_date],
        'END_OF_SHIFT' : [int(eos)]
    }, index=['line'])

    df_insert.to_sql('zone4', con=db, if_exists='append', index=False)