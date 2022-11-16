import common.helper_functions as helper_functions
import logging
import pandas as pd
import warnings
import numpy as np

warnings.filterwarnings("ignore")

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
                convert_tz(START_TIME,'US/Pacific','GMT') AS START_DATE_TIME,
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
        logging.info(bonder)
        logging.info(len(sub_df))
        for row in sub_df.itertuples(False,'Tuples'):
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
    
    ict_con.close()

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
                <td style="text-align:left"><strong>QUAD A</strong></td>
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