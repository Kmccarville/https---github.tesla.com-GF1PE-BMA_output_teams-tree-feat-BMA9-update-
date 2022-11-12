import common.helper_functions as helper_functions

from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import pytz
import pymsteams
import warnings

warnings.filterwarnings("ignore")

#bonder ideal ct
def query_ideal_ct_data(db):
    query = """
            SELECT PRODUCTION_LINE AS LINE,UUT_MODEL,IDEAL_CYCLE_TIME/60 AS CT FROM m3_teep.ideal_cycle_times
            WHERE MACHINE_TYPE='BONDER'
            AND REVISION=(SELECT MAX(REVISION) FROM m3_teep.ideal_cycle_times WHERE MACHINE_TYPE='BONDER')
            """
    ct_df = pd.read_sql(query,db)
    return ct_df
    
#pull bonder actual and ideal cycle times
def query_bonder_ct(db,start,end, ideal_ct_df, line_list):

    def get_ideal_ct(ideal_ct_df,line,model):
        ct_query = ideal_ct_df.query(f"LINE=='{line}' and UUT_MODEL=='{model}'",engine='python')
        if len(ct_query):
            ct_ideal = ct_query.iloc[0]['CT']
        else:
            ct_ideal = 0
        return ct_ideal
        
    ct_query = f"""
    SELECT
    left(a.name,4) as LINE,
    CASE
    WHEN p.partnumber in ('1091593-01-H','1609293-01-A','1609293-02-A') THEN 'E3S'
    WHEN p.partnumber in ('1091594-01-H','1609294-01-A','1609294-02-A') THEN 'E3L'
    WHEN p.partnumber in ('1609905-01-A','1609906-01-A','1609906-02-A') THEN 'E1'
    END AS MODEL,
    AVG(MINUTE(timediff(tp.completed,tp.started))) as CT,
    COUNT(MINUTE(timediff(tp.completed,tp.started))) as MOD_COUNT
    FROM sparq.thing t
    JOIN sparq.part p on p.id = t.partid
    JOIN sparq.thingpath tp on tp.thingid = t.id
    JOIN sparq.actor a on a.id = tp.actormodifiedby
    WHERE tp.flowstepname = '3BM-52000'
    AND tp.completed BETWEEN '{start}' and '{end}'
    GROUP BY 1,2
    """
    df = pd.read_sql(ct_query,db)
    
    ct_df = pd.DataFrame({'LINE' : [], 'CT' : []})
    i_ct_df = pd.DataFrame({'LINE' : [], 'I_CT' : []})
    row = []
    for line in line_list:
        sub_df = df.query(f"LINE=='{line}'")
        mod_count = 0
        count_x_ct = 0
        count_x_ict = 0
        for row in sub_df.itertuples(False,'Tuples'):
            mod_count += row.MOD_COUNT
            ideal_ct = get_ideal_ct(ideal_ct_df,line,row.MODEL)
            count_x_ct += row.MOD_COUNT*row.CT
            count_x_ict += row.MOD_COUNT*ideal_ct
        if mod_count:
            avg_ct = round(count_x_ct/mod_count)
            avg_i_ct = round(count_x_ict/mod_count)
        else:
            avg_ct = 'null'
            avg_i_ct = 'null'
        row = [line,avg_ct]
        i_row = [line,avg_i_ct]
        ct_df.loc[len(ct_df)] = row
        i_ct_df.loc[len(i_ct_df)] = i_row
    
    return ct_df,i_ct_df

def bonder_main(shift_start,shift_end):
    def query_bonder_list(db):
        query = f"""
                SELECT MACHINE_ID
                FROM
                m3_bm_process_parameters.static_station
                WHERE 
                IN_USE=1 
                AND MACHINE_ID like '3BM_-52000-___'
                AND left(MACHINE_ID,3) = '3BM'
                """
        return pd.read_sql(query,db)    

    def query_bonder_logs(db,shift_start,shift_end):
        query = f"""
                SELECT 
                MACHINE_ID,
                START_TIME AS START_DATE_TIME,
                LANE_F_EM_STEP,
                LANE_R_EM_STEP,
                FAULT_CODE,
                BONDER_MODE,
                BONDER_STATUS,
                BONDER_ASSY_COMP,
                LANE_F_MODE,
                LANE_R_MODE
                FROM m3_teep_v3.wirebond_logs t
                WHERE 
                START_TIME > convert_tz('{shift_start}','GMT','US/Pacific')
                AND START_TIME < convert_tz('{shift_end}','GMT','US/Pacific')
                AND left(MACHINE_ID,3) = '3BM'
                ORDER BY 2 ASC
                """
        return pd.read_sql(query,db)

    def query_first_log(db,shift_start,machine_id):
        query = f"""
                SELECT 
                MACHINE_ID,
                CONVERT('{shift_start}',DATETIME) as START_DATE_TIME,
                LANE_F_EM_STEP,
                LANE_R_EM_STEP,
                FAULT_CODE,
                BONDER_MODE,
                BONDER_STATUS,
                BONDER_ASSY_COMP,
                LANE_F_MODE,
                LANE_R_MODE
                FROM m3_teep_v3.wirebond_logs t
                WHERE 
                START_TIME < convert_tz('{shift_start}','GMT','US/Pacific')
                AND MACHINE_ID = '{machine_id}'
                ORDER BY START_TIME DESC
                LIMIT 1;
                """
        df = pd.read_sql(query,db)
        return df
        
    def get_total_time(sub_df,col_name):
        # sub_logs_df_sub = sub_df.query(f"{col_name}==1")
        sub_logs_df_sub = sub_df[sub_df[col_name].astype(int)==1]
        if len(sub_logs_df_sub):
            total_time = int(round(sub_logs_df_sub.groupby(col_name)['CT_SEC'].sum().iloc[0],0))
        else:
            total_time = 0
        
        return total_time
    
    #parse dataframes for line-based value
    def get_teep_val(df,line,quad,u_or_a):
        if len(df):
            col = 'UTILIZATION' if u_or_a == 'U' else 'AVAILABILITY'
            sub_df = df.query(f"LINE=={line} and QUAD=='{quad}'")
            val = sub_df.iloc[0][col] if len(sub_df) else 0
        else:
            val = 0
        return val

    time_duration = (shift_end - shift_start).seconds

    #define globals for em steps and fault codes
    GOOD_EM_STEPS = [11140,11240]
    GOOD_EM_STEPS_L5 = [20050,30050]
    FINAL_EM_STEPS = [11150,11250]
    FINAL_EM_STEPS_L5 = [0]
    GOOD_FAULT_CODES = [0,10,11]
    BONDTOOL_FAULT_CODE = 214
    #create connection
    ict_con = helper_functions.get_sql_conn('interconnect_eng')
    bonders = query_bonder_list(ict_con)
    bonder_logs_df = query_bonder_logs(ict_con,shift_start,shift_end)

    teep_df = pd.DataFrame({})
    for bonder in bonders['MACHINE_ID']:  
        #get bond counts
        #set EM step values based on line
        good_em_steps = GOOD_EM_STEPS_L5 if '3BM5' in bonder else GOOD_EM_STEPS
        final_em_steps = FINAL_EM_STEPS_L5 if '3BM5' in bonder else FINAL_EM_STEPS
        s_b_steps = final_em_steps + [0]
        
        #get subset of bonder logs
        sub_df = bonder_logs_df.query(f"MACHINE_ID=='{bonder}'")
        #get first log before shift start
        first_df = query_first_log(ict_con,shift_start,bonder)
        #add to existing df
        sub_df = pd.concat([first_df,sub_df],axis=0)
        #assign EM STEP as greater of LANE EM STEPs
        sub_df.loc[:,'EM_STEP'] = np.where(sub_df['LANE_F_EM_STEP'] >= sub_df['LANE_R_EM_STEP'],sub_df['LANE_F_EM_STEP'],sub_df['LANE_R_EM_STEP'])
        sub_df.loc[:,'BONDTOOL_CHANGE'] = 0
        
        #assign columns for each unique fault code excluding bondtool changes
        unique_faults = set(sub_df['FAULT_CODE'])
        ignore_faults = GOOD_FAULT_CODES + [BONDTOOL_FAULT_CODE]
        unique_faults = [fault for fault in unique_faults if fault not in ignore_faults]
        for fault_code in unique_faults:
            sub_df.loc[:,fault_code] = 0
        
        #assign end times based on start time of next row
        sub_df['END_DATE_TIME'] = sub_df['START_DATE_TIME'].shift(-1)
        #remove rows where start and end time are the same (for the time buffer)
        sub_df = sub_df.query("START_DATE_TIME!=END_DATE_TIME")
        #assign the shift end time to the last row's end time
        sub_df.iloc[-1,sub_df.columns.get_loc('END_DATE_TIME')] = shift_end
        #derive time between start and end times
        sub_df['CT_SEC'] = (sub_df['END_DATE_TIME'] - sub_df['START_DATE_TIME']).astype('timedelta64[ms]')/1000\
            
        #get all unique faults and setup dictinoary to track fault statuses
        unique_faults_sub = set(sub_df['FAULT_CODE'])
        unique_faults_sub = [fault for fault in unique_faults_sub if fault not in ignore_faults]
        status_dict = {fault:0 for fault in unique_faults_sub}
        
        #loop through rows to determine when bondtool change or fault starts
        #continue status of the fault until condition is met
        bondtool_change = 0
        bt_instances = 0
        for row in sub_df.itertuples('Tuples'):
            #BOND TOOL CHANGE LOGIC
            if row.FAULT_CODE == BONDTOOL_FAULT_CODE and row.BONDER_MODE == 0:
                bondtool_change+=1
                sub_df.loc[row.Index,'BONDTOOL_CHANGE']=1
                if bondtool_change == 1:
                    bt_instances += 1
            if bondtool_change!=0 and row.BONDER_MODE==0:
                sub_df.loc[row.Index,'BONDTOOL_CHANGE']=1
            if row.BONDER_MODE==1:
                bondtool_change=0
        
            #Resets for all non-bondtool change statuses
            if row.EM_STEP not in good_em_steps or (row.BONDER_STATUS==3 and row.CT_SEC>1 and row.FAULT_CODE in GOOD_FAULT_CODES):
                status_dict = {fault:0 for fault in unique_faults_sub}
                
            for fault_code in unique_faults_sub:
                # if row.FAULT_CODE == fault_code and row.EM_STEP in GOOD_EM_STEPS:
                if row.FAULT_CODE == fault_code:
                    status_dict = {fault:0 for fault in unique_faults_sub}
                    status_dict[fault_code] += 1
                    sub_df.loc[row.Index,fault_code]=1 
                if status_dict[fault_code]!=0 and (row.BONDER_STATUS!=3 or row.BONDER_MODE!=1 or row.CT_SEC<1):
                    sub_df.loc[row.Index,fault_code]=1
                    
        
        #assign statuses based on inputs
        sub_df.loc[:,'FAULT_VAL'] = sub_df[[col for col in sub_df.columns if col in unique_faults_sub]].sum(axis=1)
        
        sub_df.loc[:,'EM_FAULT'] = np.where(((sub_df['BONDTOOL_CHANGE']==0) | (sub_df['FAULT_VAL']==0)) & (sub_df['EM_STEP']%2==1),1,0)
        
        sub_df.loc[:,'ROUTING_ON'] = np.where((sub_df['LANE_F_MODE']>0) | (sub_df['LANE_R_MODE']>0),1,0)
        sub_df.loc[:,'ROUTING_OFF'] = np.where((sub_df['LANE_F_MODE']==0) & (sub_df['LANE_R_MODE']==0),1,0)
        
        sub_df.loc[:,'FAULTED'] = np.where(((sub_df['BONDTOOL_CHANGE']==1) | (sub_df['FAULT_VAL']>0) | (sub_df['EM_FAULT']==1)) \
                                        & (sub_df['ROUTING_ON']==1),1,0)
            
        sub_df.loc[:,'WAIT_ASSY_COMP'] = np.where((sub_df['FAULTED']==0) & (sub_df['EM_STEP'].isin(good_em_steps)) \
                                                & (sub_df['BONDER_STATUS']!=3) & (sub_df['BONDER_ASSY_COMP']==0) \
                                                & (sub_df['ROUTING_ON']==1),1,0)
        
        sub_df.loc[:,'NOT_READY'] = np.where((sub_df['FAULTED']==0) & (sub_df['EM_STEP'].isin(good_em_steps)) \
                                                        & (sub_df['BONDER_STATUS']!=3) & (sub_df['BONDER_ASSY_COMP']==1) \
                                                            & (sub_df['ROUTING_ON']==1),1,0)
            
        sub_df.loc[:,'NOT_BONDING'] = np.where((sub_df['FAULTED']==0) & (sub_df['EM_STEP']==0) & (sub_df['BONDER_MODE']==1) \
                                                        & (sub_df['BONDER_STATUS']==3) & (sub_df['BONDER_ASSY_COMP']==1) \
                                                            & (sub_df['ROUTING_ON']==1),1,0)
        
        sub_df.loc[:,'OUT_OF_AUTO'] = np.where((sub_df['FAULTED']==0) & (sub_df['EM_STEP']==0) & (sub_df['BONDER_MODE']==0) & (sub_df['ROUTING_ON']==1),1,0)
        
        sub_df.loc[:,'UNAVAILABLE'] = np.where((sub_df['FAULTED']==1) | (sub_df['ROUTING_OFF']==1) | (sub_df['WAIT_ASSY_COMP']==1) | \
                                            (sub_df['NOT_READY']==1) | (sub_df['NOT_BONDING']==1) | (sub_df['OUT_OF_AUTO']==1),1,0)
            
        sub_df.loc[:,'BONDING'] = np.where((sub_df['UNAVAILABLE']==0) & (sub_df['BONDER_STATUS']==3) & (sub_df['BONDER_ASSY_COMP']==0),1,0)
        
        sub_df.loc[:,'PP_CHANGE'] = np.where(((sub_df['UNAVAILABLE']==0) & (~sub_df['EM_STEP'].isin(good_em_steps)) & (~sub_df['EM_STEP'].isin(s_b_steps)) & (sub_df['EM_STEP']%2==0)) & (sub_df['BONDER_STATUS']!=3),1,0)
            
        sub_df.loc[:,'IN_ASSY_COMP'] = np.where((sub_df['UNAVAILABLE']==0) & (sub_df['EM_STEP'].isin(good_em_steps)) & (sub_df['BONDER_ASSY_COMP']==1),1,0)
        
        sub_df.loc[:,'STARVE_BLOCK'] = np.where(((sub_df['UNAVAILABLE']==0) & (sub_df['EM_STEP'].isin(s_b_steps)) & (sub_df['BONDER_MODE']==1)  & (sub_df['BONDER_STATUS']!=3)) \
                                                | ((sub_df['UNAVAILABLE']==0) & (sub_df['EM_STEP'].isin(s_b_steps)) & (sub_df['BONDER_MODE']==1)  & (sub_df['BONDER_STATUS']==3)),1,0)
        
        #get total times for each column and assign to tag
        routing_off_time = get_total_time(sub_df,'ROUTING_OFF')
        not_ready_time = get_total_time(sub_df,'NOT_READY')
        wait_assy_comp_time= get_total_time(sub_df,'WAIT_ASSY_COMP')
        ooa_time = get_total_time(sub_df,'OUT_OF_AUTO')   
        total_fault_time = get_total_time(sub_df,'FAULTED')
        not_bond_time = get_total_time(sub_df,'NOT_BONDING')
        pp_change_time = get_total_time(sub_df,'PP_CHANGE')
        assy_comp_time = get_total_time(sub_df,'IN_ASSY_COMP')
        starve_block_time = get_total_time(sub_df,'STARVE_BLOCK')
        
        #calculate summary data
        unavailable_time = pp_change_time + assy_comp_time + not_bond_time + total_fault_time + ooa_time + wait_assy_comp_time + not_ready_time + routing_off_time
        utilization = round((time_duration - starve_block_time)/time_duration,3)
        availability = round((time_duration - unavailable_time - starve_block_time)/(time_duration - starve_block_time),3) if starve_block_time < time_duration else np.nan

        teep_sub = pd.DataFrame({
                                'MACHINE_ID' : [bonder],
                                'UTILIZATION' : [utilization],
                                'AVAILABILITY' : [availability]
                                })

        teep_df = pd.concat([teep_df,teep_sub],axis=0)
        

    teep_df.loc[:,'LINE'] = teep_df['MACHINE_ID'].str.split('-').str[0].str.split('3BM').str[-1].astype('int64')
    teep_df.loc[:,'BOND_NUM'] = teep_df['MACHINE_ID'].str.split('-').str[-1].str[-3:-1].astype('int64')

    teep_df.loc[:,'QUAD'] = np.where((teep_df['LINE'] < 4 ) & (teep_df['BOND_NUM'] < 6), 'A',
                            np.where((teep_df['LINE'] < 4 ) & (teep_df['BOND_NUM'] >= 6) & (teep_df['BOND_NUM'] < 11), 'B',
                            np.where((teep_df['LINE'] < 4 ) & (teep_df['BOND_NUM'] >= 11)& (teep_df['BOND_NUM'] < 17) , 'C',
                            np.where((teep_df['LINE'] < 4 ) & (teep_df['BOND_NUM'] >= 17), 'D',
                            np.where((teep_df['LINE'] == 4) & (teep_df['BOND_NUM'] >= 1) & (teep_df['BOND_NUM'] < 7), 'A',
                            np.where((teep_df['LINE'] == 4) & (teep_df['BOND_NUM'] >= 7) & (teep_df['BOND_NUM'] < 13), 'B',
                            np.where((teep_df['LINE'] == 4) & (teep_df['BOND_NUM'] >= 13) , 'C',
                            np.where((teep_df['LINE'] == 5) & (teep_df['BOND_NUM'] % 2 == 1), 'A',
                            np.where((teep_df['LINE'] == 5) & (teep_df['BOND_NUM'] % 2 == 0), 'B','NONE')))))))))


    teep_df_group = teep_df.groupby(['LINE','QUAD'])['UTILIZATION','AVAILABILITY'].mean().reset_index()
    teep_df_group.loc[:,'UTILIZATION'] = round(teep_df_group['UTILIZATION']*100,1)
    teep_df_group.loc[:,'AVAILABILITY'] = round(teep_df_group['AVAILABILITY']*100,1)

    LINES = ['3BM1','3BM2','3BM3','3BM4','3BM5']
    header_html = """<th></th>"""
    quad_a_html = """
                <tr>
                <td style="text-align:left"><strong> A</strong></td>
                """
    quad_b_html = """
                <tr>
                <td style="text-align:left"><strong>QUAD B</strong></td>
                """
    quad_c_html = """
                <tr>
                <td style="text-align:left"><strong>QUAD C</strong></td>
                """
    quad_d_html = """
                <tr>
                <td style="text-align:left"><strong>QUAD D</strong></td>
                """
    for line in LINES:
        line_num = int(line[-1])
        header_html += f"""<th style="text-align:center">{line}</th>"""
        quad_a_a = get_teep_val(teep_df_group,line_num,'A','A')
        quad_b_a = get_teep_val(teep_df_group,line_num,'B','A')
        quad_c_a = get_teep_val(teep_df_group,line_num,'C','A') if line_num < 5 else '---'
        quad_d_a = get_teep_val(teep_df_group,line_num,'D','A') if line_num < 4 else '---'
        quad_a_html += f"""<td style="text-align:center">{quad_a_a}</td>"""
        quad_b_html += f"""<td style="text-align:center">{quad_b_a}</td>"""
        quad_c_html += f"""<td style="text-align:center">{quad_c_a}</td>"""
        quad_d_html += f"""<td style="text-align:center">{quad_d_a}</td>"""

    quad_a_html += "</tr>"
    quad_b_html += "</tr>"
    quad_c_html += "</tr>"
    quad_d_html += "</tr>"

    wb_a_html = '<table>' + "<caption>Bonder Availability</caption>" + header_html + quad_a_html + quad_b_html + quad_c_html + quad_d_html + '</table>'
    return wb_a_html

def main(env,eos=False):
    #begin by defining timestamps
    lookback=12 if eos else 1
    now=datetime.utcnow()
    logging.info("Z3 start %s" % datetime.utcnow())
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    seconds_between = (end - start).seconds
    #define global variables
    LINES = ['3BM1','3BM2','3BM3','3BM4','3BM5']
    INGRESS_PATHS = [
                    '[3BM01_50000_00]01/_OEE_Reporting/TSMs/InputStation',
                    '[3BM02_50000_00]02/_OEE_Reporting/TSMs/InputStation',
                    '[3BM03_50000_00]03/_OEE_Reporting/TSMs/InputStation',
                    '[3BM04_01_50000_00]04_01/_OEE_Reporting/TSMs/Ingress01_IngressTransfer',
                    '[3BM04_02_50000_00]04_02/_OEE_Reporting/TSMs/Ingress02_IngressTransfer',
                    '[3BM04_03_50000_00]04_03/_OEE_Reporting/TSMs/Ingress03_IngressTransfer',
                    '[3BM5-50100-01]TSL0091 Ingress/Main/TSM/LatchFaultReporting'
                    ]
    PO_PATHS = [
                '[3BM01_50000_00]01/_OEE_Reporting/TSMs/Packout1_Packout',
                '[3BM02_50000_00]02/_OEE_Reporting/TSMs/Packout1_Packout',
                '[3BM03_50000_00]03/_OEE_Reporting/TSMs/Packout1_Packout',
                '[3BM04_57000_01]_OEE_Reporting/TSMs/Main',
                '[3BM04_50000]3BM05_57000/_OEE_Reporting/Packout_MTR']

    PO_FLOWSTEP = '3BM-57000'
    flowsteps = [PO_FLOWSTEP]

    #establish db connections

    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    plc_con = helper_functions.get_sql_conn('plc_db')
    ict_con = helper_functions.get_sql_conn('interconnect_ro')

    df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)
    ideal_ct_df = query_ideal_ct_data(ict_con)
    if not eos:
        wb_ct_df,wb_i_ct_df = query_bonder_ct(mos_con,start,end,ideal_ct_df,LINES)
    else:
        wb_ct_df = pd.DataFrame({})
        wb_i_ct_df = pd.DataFrame({})

    ing_df = helper_functions.query_tsm_state(plc_con,start, end, INGRESS_PATHS, 'Starved')
    po_df = helper_functions.query_tsm_state(plc_con,start, end, PO_PATHS, 'Starved',1)    
    mos_con.close()
    plc_con.close()
    ict_con.close()

    header_html = ""
    output_value_html = """
                        <tr>
                        <td style="text-align:left"><b>Carsets</b></td>
                        """
    starved_wip_html = """
                        <tr>
                        <td style="text-align:left"><b>Ingress</b></td>
                        """
    starved_mtr_html = """
                    <tr>
                    <td style="text-align:left"><b>PO MTRs</b></td>
                    """
    wb_ct_html = """
                <tr>
                <td style="text-align:left"><b>Actual CT</b></td>
                """
    wb_i_ct_html = """
                    <tr>
                    <td style="text-align:left"><b>Ideal CT</b></td>
                    """
    total_output = helper_functions.get_output_val(df_output,PO_FLOWSTEP)/4
    for line in LINES:
        header_html += f"""<th style="text-align:center">{line}</th>"""
        output_val = helper_functions.get_output_val(df_output,PO_FLOWSTEP,line)/4
        output_value_html += f"""<td style="text-align:center">{output_val:.1f}</td>"""
        #divide by 3 if it's line 4 because there are 4 ingress stations
        ingress_divisor = 3 if line=='3BM4' else 1
        starved_wip_html += f"""<td style="text-align:center">{helper_functions.get_val(ing_df,line,'LINE','Duration')/ingress_divisor/seconds_between*100:.0f}%</td>"""
        starved_mtr_html += f"""<td style="text-align:center">{helper_functions.get_val(po_df,line,'LINE','Duration')/seconds_between*100:.0f}%</td>"""

        actual_ct = helper_functions.get_val(wb_ct_df,line,'LINE','CT')
        ideal_ct = helper_functions.get_val(wb_i_ct_df,line,'LINE','I_CT')
        color_str = "color:red;" if actual_ct > ideal_ct else ""

        wb_ct_html += f"""<td style="text-align:center;{color_str}">{actual_ct}</td>"""
        wb_i_ct_html += f"""<td style="text-align:center;">{ideal_ct}</td>"""

    output_header = "<tr>" + """<th style="text-align:center"></th>""" + header_html + """<th style="text-align:center">TOTAL</th></tr>"""
    starved_header = "<tr>" + """<th style="text-align:center"></th>""" + header_html + "</tr>"
    wb_header = "<tr>" + """<th style="text-align:center"></th>""" + header_html + "</tr>"

    output_value_html += f"""<td style="text-align:center"><b>{total_output:.1f}</b></td></tr>"""
    starved_wip_html += "</tr>"
    starved_mtr_html += "</tr>"

    wb_ct_html += "</tr>"
    wb_i_ct_html += "</tr>"


    output_html = "<table>" + output_header + output_value_html + "</table>"
    starved_html = "<table>" + "<caption>Starvation %</caption>" + starved_header + starved_wip_html + starved_mtr_html + "</table>"
    wb_html = "<table>" + "<caption>Bonder Cycle Time (mins)</caption>" +  wb_header + wb_ct_html + wb_i_ct_html + "</table>"

    wb_teep_html = bonder_main(start,end)

    webhook_key = 'teams_webhook_Zone3_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']
    
    #making the hourly teams message
    teams_msg = pymsteams.connectorcard(webhook)
    title = 'Zone 3 EOS Report' if eos else 'Zone 3 Hourly Update'
    teams_msg.title(title)
    teams_msg.summary('summary')
    K8S_BLUE = '#3970e4'
    TESLA_RED = '#cc0000'
    msg_color = TESLA_RED if eos else K8S_BLUE
    teams_msg.color(msg_color)
    #make a card with output data
    output_card = pymsteams.cardsection()
    output_card.text(output_html)
    teams_msg.addSection(output_card)
    #make a card with starvation data
    starved_card = pymsteams.cardsection()
    starved_card.text(starved_html)
    teams_msg.addSection(starved_card)
    #make a card with starvation data
    if not eos:
        wb_card = pymsteams.cardsection()
        wb_card.text(wb_teep_html)
        teams_msg.addSection(wb_card)
    #add a link to the confluence page
    teams_msg.addLinkButton("Questions?", "https://confluence.teslamotors.com/display/PRODENG/Battery+Module+Hourly+Update")
    teams_msg.send()