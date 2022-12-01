import common.helper_functions as helper_functions

from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import pytz
import pymsteams
import warnings
import numpy as np
from resources import z3_wb_teep

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

def query_bonder_list(db):
    query = f"""
            SELECT 
            MACHINE_ID,
            OBTC
            FROM
            m3_bm_process_parameters.static_station
            WHERE 
            IN_USE=1 
            AND MACHINE_ID like '3BM_-52000-___'
            AND left(MACHINE_ID,3) = '3BM'
            """
    return pd.read_sql(query,db)    

def query_bonder_logs(db,start,end):
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
            START_TIME > convert_tz('{start}','GMT','US/Pacific')
            AND START_TIME < convert_tz('{end}','GMT','US/Pacific')
            AND left(MACHINE_ID,3) = '3BM'
            ORDER BY 2 ASC
            """
    return pd.read_sql(query,db)

def query_first_log(db,start,machine_id):
    query = f"""
            SELECT 
            MACHINE_ID,
            CONVERT('{start}',DATETIME) as START_DATE_TIME,
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
            START_TIME < convert_tz('{start}','GMT','US/Pacific')
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

def get_mttr_table(db,start,end):
    bonder_start = start+timedelta(hours=-2)
    BONDTOOL_FAULT_CODE = 214
    bonders = query_bonder_list(db)
    bonder_logs_df = query_bonder_logs(db,bonder_start,end)

    all_times_df = pd.DataFrame({})
    for bonder in bonders['MACHINE_ID']:   
        #get subset of bonder logs
        sub_df = bonder_logs_df.query(f"MACHINE_ID=='{bonder}'")
        first_df = query_first_log(db,start,bonder)
        #add to existing df
        sub_df = pd.concat([first_df,sub_df],axis=0)
        #assign EM STEP as greater of LANE EM STEPs
        sub_df.loc[:,'EM_STEP'] = np.where(sub_df['LANE_F_EM_STEP'] >= sub_df['LANE_R_EM_STEP'],sub_df['LANE_F_EM_STEP'],sub_df['LANE_R_EM_STEP'])
        sub_df.loc[:,'BONDTOOL_CHANGE'] = 0
        
        #assign end times based on start time of next row
        sub_df['END_DATE_TIME'] = sub_df['START_DATE_TIME'].shift(-1)
        #remove rows where start and end time are the same (for the time buffer)
        sub_df = sub_df.query("START_DATE_TIME!=END_DATE_TIME")
        #assign the shift end time to the last row's end time
        sub_df.iloc[-1,sub_df.columns.get_loc('END_DATE_TIME')] = end
        #derive time between start and end times
        sub_df['CT_SEC'] = (sub_df['END_DATE_TIME'] - sub_df['START_DATE_TIME']).astype('timedelta64[ms]')/1000    
        #loop through rows to determine when bondtool change or fault starts
        #continue status of the fault until condition is met
        bondtool_change = 0
        bt_change_times = []
        in_auto_times = []
        for row in sub_df.itertuples(True,'Tuples'):
            #BOND TOOL CHANGE LOGIC
            if row.FAULT_CODE == BONDTOOL_FAULT_CODE and row.BONDER_MODE == 0:
                bondtool_change+=1
                if bondtool_change == 1:
                    bt_change_times.append(row.START_DATE_TIME)
            if row.BONDER_STATUS==3 and row.BONDER_MODE==1 and bondtool_change>0:
                bondtool_change=0
                in_auto_times.append(row.START_DATE_TIME)
                
        delta = len(bt_change_times) - len(in_auto_times)
        for i in range(delta):
            in_auto_times.append(end)
        
        machine_list = [bonder]*len(in_auto_times)
        times_df = pd.DataFrame({
                                    'MACHINE_ID'     : machine_list,
                                    'BT_CHANGE_TIME' : bt_change_times,
                                    'IN_AUTO_TIME' : in_auto_times
                                    })
        
        all_times_df = pd.concat([all_times_df,times_df],axis=0,ignore_index=True)
            
    all_times_df_filter = all_times_df[(all_times_df['IN_AUTO_TIME']>=start) & (all_times_df['IN_AUTO_TIME']<end)]

    bt_df = pd.DataFrame({})
    for row in all_times_df_filter.itertuples(True,'Tuples'):
        query = f"""
                SELECT
                    EquipmentID as MACHINE_ID,
                    CONVERT('{row.BT_CHANGE_TIME}',DATETIME) as BT_START_TIME,
                    convert_tz(Time_Stamp,'US/Pacific','GMT') as BT_COMPLETE_TIME,
                    Bond_Count1 as BC1,
                    Bond_Count2 as BC2,
                    Bond_Count3 as BC3,
                    Bond_Count4 as BC4,
                    Bond_Count5 as BC5,
                    Bond_Count6 as BC6
                FROM
                    m3_wirebond.bond_counter
                WHERE 
                    Time_Stamp < (convert_tz('{row.IN_AUTO_TIME}','GMT','US/Pacific') + interval 60 second)
                    AND EquipmentID = '{row.MACHINE_ID}'
                    AND Source = 'Ready To Running'
                ORDER BY ID DESC
                LIMIT 1
                """
        df = pd.read_sql(query,db)
        bt_df = pd.concat([bt_df,df],axis=0)

    bt_df = bt_df.merge(bonders,how='left',on='MACHINE_ID')
    bt_df.fillna(0,inplace=True)

    #define ideal cycle time for each consumabel change in seconds
    BT_IDEAL = 450
    WG_IDEAL = 196
    CB_IDEAL = 190
    FT_IDEAL = 310
    SS_IDEAL = 479
    #define threshold that count needs to be below to be counted as a change
    COUNT_THRESHOLD = 100

    bt_df.loc[:,'BT'] = np.where(((bt_df['BC1'] < COUNT_THRESHOLD) & (bt_df['OBTC']==0)) | ((bt_df['BC2'] < COUNT_THRESHOLD) & (bt_df['OBTC']==1)),1,0)
    bt_df.loc[:,'WG'] = np.where(bt_df['BC3'] < COUNT_THRESHOLD,1,0)
    bt_df.loc[:,'CB'] = np.where(bt_df['BC4'] < COUNT_THRESHOLD,1,0)
    bt_df.loc[:,'FT'] = np.where(bt_df['BC5'] < COUNT_THRESHOLD,1,0)
    bt_df.loc[:,'SS'] = np.where(bt_df['BC6'] < COUNT_THRESHOLD,1,0)

    bt_df.loc[:,'IDEAL_SEC'] = bt_df['BT']*BT_IDEAL + bt_df['WG']*WG_IDEAL + bt_df['CB']*CB_IDEAL + bt_df['FT']*FT_IDEAL + np.where(bt_df['BT']==0,bt_df['SS']*SS_IDEAL,0)
    bt_df.loc[:,'ACTUAL_SEC'] = (bt_df['BT_COMPLETE_TIME'] - bt_df['BT_START_TIME']).dt.total_seconds()
    bt_df.loc[:,'LOST_SEC'] = bt_df['ACTUAL_SEC'] - bt_df['IDEAL_SEC']

    bt_df.loc[:,'LINE'] = bt_df['MACHINE_ID'].str.split('-').str[0].str.split('3BM').str[-1].astype('int64')
    bt_df.loc[:,'BOND_NUM'] = bt_df['MACHINE_ID'].str.split('-').str[-1].str[-3:-1].astype('int64')

    bt_df.loc[:,'QUAD'] = np.where((bt_df['LINE'] < 4 ) & (bt_df['BOND_NUM'] < 6), 'A',
                            np.where((bt_df['LINE'] < 4 ) & (bt_df['BOND_NUM'] >= 6) & (bt_df['BOND_NUM'] < 11), 'B',
                            np.where((bt_df['LINE'] < 4 ) & (bt_df['BOND_NUM'] >= 11)& (bt_df['BOND_NUM'] < 17) , 'C',
                            np.where((bt_df['LINE'] < 4 ) & (bt_df['BOND_NUM'] >= 17), 'D',
                            np.where((bt_df['LINE'] == 4) & (bt_df['BOND_NUM'] >= 1) & (bt_df['BOND_NUM'] < 7), 'A',
                            np.where((bt_df['LINE'] == 4) & (bt_df['BOND_NUM'] >= 7) & (bt_df['BOND_NUM'] < 13), 'B',
                            np.where((bt_df['LINE'] == 4) & (bt_df['BOND_NUM'] >= 13) , 'C',
                            np.where((bt_df['LINE'] == 5) & (bt_df['BOND_NUM'] % 2 == 1), 'A',
                            np.where((bt_df['LINE'] == 5) & (bt_df['BOND_NUM'] % 2 == 0), 'B','NONE')))))))))


    bt_summary = bt_df.groupby(['LINE','QUAD'])['IDEAL_SEC','ACTUAL_SEC'].sum().reset_index()
    bt_summary.loc[:,'IDEAL_MIN'] = bt_summary['IDEAL_SEC']/60
    bt_summary.loc[:,'ACTUAL_MIN'] = bt_summary['ACTUAL_SEC']/60


    quad1a_target = round(helper_functions.get_val_2(bt_summary,1,'LINE','A','QUAD','IDEAL_MIN'),1)
    quad1b_target = round(helper_functions.get_val_2(bt_summary,1,'LINE','B','QUAD','IDEAL_MIN'),1)
    quad1c_target = round(helper_functions.get_val_2(bt_summary,1,'LINE','C','QUAD','IDEAL_MIN'),1)
    quad1d_target = round(helper_functions.get_val_2(bt_summary,1,'LINE','D','QUAD','IDEAL_MIN'),1)

    quad2a_target = round(helper_functions.get_val_2(bt_summary,2,'LINE','A','QUAD','IDEAL_MIN'),1)
    quad2b_target = round(helper_functions.get_val_2(bt_summary,2,'LINE','B','QUAD','IDEAL_MIN'),1)
    quad2c_target = round(helper_functions.get_val_2(bt_summary,2,'LINE','C','QUAD','IDEAL_MIN'),1)
    quad2d_target = round(helper_functions.get_val_2(bt_summary,2,'LINE','D','QUAD','IDEAL_MIN'),1)
    
    quad3a_target = round(helper_functions.get_val_2(bt_summary,3,'LINE','A','QUAD','IDEAL_MIN'),1)
    quad3b_target = round(helper_functions.get_val_2(bt_summary,3,'LINE','B','QUAD','IDEAL_MIN'),1)
    quad3c_target = round(helper_functions.get_val_2(bt_summary,3,'LINE','C','QUAD','IDEAL_MIN'),1)
    quad3d_target = round(helper_functions.get_val_2(bt_summary,3,'LINE','D','QUAD','IDEAL_MIN'),1)

    quad4a_target = round(helper_functions.get_val_2(bt_summary,4,'LINE','A','QUAD','IDEAL_MIN'),1)
    quad4b_target = round(helper_functions.get_val_2(bt_summary,4,'LINE','B','QUAD','IDEAL_MIN'),1)
    quad4c_target = round(helper_functions.get_val_2(bt_summary,4,'LINE','C','QUAD','IDEAL_MIN'),1)

    quad5a_target = round(helper_functions.get_val_2(bt_summary,5,'LINE','A','QUAD','IDEAL_MIN'),1)
    quad5b_target = round(helper_functions.get_val_2(bt_summary,5,'LINE','B','QUAD','IDEAL_MIN'),1)

    #
    quad1a_actual = round(helper_functions.get_val_2(bt_summary,1,'LINE','A','QUAD','ACTUAL_MIN'),1)
    quad1b_actual = round(helper_functions.get_val_2(bt_summary,1,'LINE','B','QUAD','ACTUAL_MIN'),1)
    quad1c_actual = round(helper_functions.get_val_2(bt_summary,1,'LINE','C','QUAD','ACTUAL_MIN'),1)
    quad1d_actual = round(helper_functions.get_val_2(bt_summary,1,'LINE','D','QUAD','ACTUAL_MIN'),1)

    quad2a_actual = round(helper_functions.get_val_2(bt_summary,2,'LINE','A','QUAD','ACTUAL_MIN'),1)
    quad2b_actual = round(helper_functions.get_val_2(bt_summary,2,'LINE','B','QUAD','ACTUAL_MIN'),1)
    quad2c_actual = round(helper_functions.get_val_2(bt_summary,2,'LINE','C','QUAD','ACTUAL_MIN'),1)
    quad2d_actual = round(helper_functions.get_val_2(bt_summary,2,'LINE','D','QUAD','ACTUAL_MIN'),1)
    
    quad3a_actual = round(helper_functions.get_val_2(bt_summary,3,'LINE','A','QUAD','ACTUAL_MIN'),1)
    quad3b_actual = round(helper_functions.get_val_2(bt_summary,3,'LINE','B','QUAD','ACTUAL_MIN'),1)
    quad3c_actual = round(helper_functions.get_val_2(bt_summary,3,'LINE','C','QUAD','ACTUAL_MIN'),1)
    quad3d_actual = round(helper_functions.get_val_2(bt_summary,3,'LINE','D','QUAD','ACTUAL_MIN'),1)

    quad4a_actual = round(helper_functions.get_val_2(bt_summary,4,'LINE','A','QUAD','ACTUAL_MIN'),1)
    quad4b_actual = round(helper_functions.get_val_2(bt_summary,4,'LINE','B','QUAD','ACTUAL_MIN'),1)
    quad4c_actual = round(helper_functions.get_val_2(bt_summary,4,'LINE','C','QUAD','ACTUAL_MIN'),1)

    quad5a_actual = round(helper_functions.get_val_2(bt_summary,5,'LINE','A','QUAD','ACTUAL_MIN'),1)
    quad5b_actual = round(helper_functions.get_val_2(bt_summary,5,'LINE','B','QUAD','ACTUAL_MIN'),1)

    html=f"""
        <tr>
            <td></td>
            <th style="text-align:center"><strong>3BM1</strong></th>
            <th></th>
            <th style="text-align:center"><strong>3BM2</strong></th>
            <th></th>
            <th style="text-align:center"><strong>3BM3</strong></th>
            <th></th>
            <th style="text-align:center"><strong>3BM4</strong></th>
            <th></th>
            <th style="text-align:center"><strong>3BM5</strong></th>
            <th></th>
        </tr>
        <tr>
            <td></td>
            <th style="text-align:center"><strong>Actual</strong></th>
            <th style="text-align:center"><strong>Target</strong></th>
            <th style="text-align:center"><strong>Actual</strong></th>
            <th style="text-align:center"><strong>Target</strong></th>
            <th style="text-align:center"><strong>Actual</strong></th>
            <th style="text-align:center"><strong>Target</strong></th>
            <th style="text-align:center"><strong>Actual</strong></th>
            <th style="text-align:center"><strong>Target</strong></th>
            <th style="text-align:center"><strong>Actual</strong></th>
            <th style="text-align:center"><strong>Target</strong></th>
        </tr>
        <tr>
            <td style="text-align:left"><b>A</b></td>
            <td style="text-align:center">{quad1a_actual}</td>
            <td style="text-align:center">{quad1a_target}</td>
            <td style="text-align:center">{quad2a_actual}</td>
            <td style="text-align:center">{quad2a_target}</td>
            <td style="text-align:center">{quad3a_actual}</td>
            <td style="text-align:center">{quad3a_target}</td>
            <td style="text-align:center">{quad4a_actual}</td>
            <td style="text-align:center">{quad4a_target}</td>
            <td style="text-align:center">{quad5a_actual}</td>
            <td style="text-align:center">{quad5a_target}</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>B</b></td>
            <td style="text-align:center">{quad1b_actual}</td>
            <td style="text-align:center">{quad1b_target}</td>
            <td style="text-align:center">{quad2b_actual}</td>
            <td style="text-align:center">{quad2b_target}</td>
            <td style="text-align:center">{quad3b_actual}</td>
            <td style="text-align:center">{quad3b_target}</td>
            <td style="text-align:center">{quad4b_actual}</td>
            <td style="text-align:center">{quad4b_target}</td>
            <td style="text-align:center">{quad5b_actual}</td>
            <td style="text-align:center">{quad5b_target}</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>C</b></td>
            <td style="text-align:center">{quad1c_actual}</td>
            <td style="text-align:center">{quad1c_target}</td>
            <td style="text-align:center">{quad2c_actual}</td>
            <td style="text-align:center">{quad2c_target}</td>
            <td style="text-align:center">{quad3c_actual}</td>
            <td style="text-align:center">{quad3c_target}</td>
            <td style="text-align:center">{quad4c_actual}</td>
            <td style="text-align:center">{quad4c_target}</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">---</td>
        </tr>
        <tr>
            <td style="text-align:center">{quad1d_actual}</td>
            <td style="text-align:center">{quad1d_target}</td>
            <td style="text-align:center">{quad2d_actual}</td>
            <td style="text-align:center">{quad2d_target}</td>
            <td style="text-align:center">{quad3d_actual}</td>
            <td style="text-align:center">{quad3d_target}</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">---</td>
            <td style="text-align:center">---</td>
        </tr>
        """
    return html

def get_starved_table(db,start,end):
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

    seconds_between = (end - start).seconds

    ing_df = helper_functions.query_tsm_state(db,start, end, INGRESS_PATHS, 'Starved')
    po_df = helper_functions.query_tsm_state(db,start, end, PO_PATHS, 'Starved',1)   

    ing1_starved = round(helper_functions.get_val(ing_df,'3BM1','LINE','Duration')/seconds_between*100,1)
    ing2_starved = round(helper_functions.get_val(ing_df,'3BM2','LINE','Duration')/seconds_between*100,1)
    ing3_starved = round(helper_functions.get_val(ing_df,'3BM3','LINE','Duration')/seconds_between*100,1)
    ing4_starved = round(helper_functions.get_val(ing_df,'3BM4','LINE','Duration')/seconds_between*100,1)
    ing5_starved = round(helper_functions.get_val(ing_df,'3BM5','LINE','Duration')/seconds_between*100,1)

    po1_starved = round(helper_functions.get_val(po_df,'3BM1','LINE','Duration')/seconds_between*100,1)
    po2_starved = round(helper_functions.get_val(po_df,'3BM2','LINE','Duration')/seconds_between*100,1)
    po3_starved = round(helper_functions.get_val(po_df,'3BM3','LINE','Duration')/seconds_between*100,1)
    po4_starved = round(helper_functions.get_val(po_df,'3BM4','LINE','Duration')/seconds_between*100,1)
    po5_starved = round(helper_functions.get_val(po_df,'3BM5','LINE','Duration')/seconds_between*100,1)

    html=f"""
        <tr>
            <td></td>
            <th style="text-align:center"><strong>3BM1</strong></th>
            <th style="text-align:center"><strong>3BM2</strong></th>
            <th style="text-align:center"><strong>3BM3</strong></th>
            <th style="text-align:center"><strong>3BM4</strong></th>
            <th style="text-align:center"><strong>3BM5</strong></th>
        </tr>
        <tr>
            <td style="text-align:left"><b>Ingress</b></td>
            <td style="text-align:center">{ing1_starved}</td>
            <td style="text-align:center">{ing2_starved}</td>
            <td style="text-align:center">{ing3_starved}</td>
            <td style="text-align:center">{ing4_starved}</td>
            <td style="text-align:center">{ing5_starved}</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>PO MTRs</b></td>
            <td style="text-align:center">{po1_starved}</td>
            <td style="text-align:center">{po2_starved}</td>
            <td style="text-align:center">{po3_starved}</td>
            <td style="text-align:center">{po4_starved}</td>
            <td style="text-align:center">{po5_starved}</td>
        </tr>
        """
    return html


def main(env,eos=False):
    #begin by defining timestamps
    lookback=12 if eos else 1
    now=datetime.utcnow()
    logging.info("Z3 start %s" % datetime.utcnow())
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)
    
    #define global variables
    NORMAL_DIVISOR = 4
    PO_FLOWSTEP = '3BM-57000'
    flowsteps = [PO_FLOWSTEP]

    #establish db connections
    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    plc_con = helper_functions.get_sql_conn('plc_db')
    ict_con = helper_functions.get_sql_conn('interconnect_ro')

    df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)
    starve_table = get_starved_table(plc_con,start,end)
    mttr_table = get_mttr_table(ict_con,start,end)

    mos_con.close()
    plc_con.close()
    ict_con.close()

    output1 = round(helper_functions.get_output_val(df_output,PO_FLOWSTEP,'3BM1')/NORMAL_DIVISOR,1)
    output2 = round(helper_functions.get_output_val(df_output,PO_FLOWSTEP,'3BM2')/NORMAL_DIVISOR,1)
    output3 = round(helper_functions.get_output_val(df_output,PO_FLOWSTEP,'3BM3')/NORMAL_DIVISOR,1)
    output4 = round(helper_functions.get_output_val(df_output,PO_FLOWSTEP,'3BM4')/NORMAL_DIVISOR,1)
    output5 = round(helper_functions.get_output_val(df_output,PO_FLOWSTEP,'3BM5')/NORMAL_DIVISOR,1)  
    total_output = round(helper_functions.get_output_val(df_output,PO_FLOWSTEP)/NORMAL_DIVISOR,1)

    output_table=f"""
                <tr>
                    <td></td>
                    <th style="text-align:center">3BM1</th>
                    <th style="text-align:center">3BM2</th>
                    <th style="text-align:center">3BM3</th>
                    <th style="text-align:center">3BM4</th>
                    <th style="text-align:center">3BM5</th>
                    <th style="text-align:center">TOTAL</th>
                </tr>
                <tr>
                    <td style="text-align:left"><b>Carsets</b></td>
                    <td style="text-align:center">{output1}</td>
                    <td style="text-align:center">{output2}</td>
                    <td style="text-align:center">{output3}</td>
                    <td style="text-align:center">{output4}</td>
                    <td style="text-align:center">{output5}</td>
                    <td style="text-align:center"><b>{total_output}</b></td>
                </tr>
                """


    output_html = "<table>" + "<caption>Throughput</caption>" + output_table + "</table>"
    starved_html = "<table>" + "<caption>Starvation %</caption>" + starve_table + "</table>"
    wb_html = "<table>" + "<caption>Consumable Change Lost Minutes</caption>" +  mttr_table + "</table>"

    # wb_teep_html = z3_wb_teep.bonder_main(start,end)

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
    wb_card = pymsteams.cardsection()
    wb_card.text(wb_html)
    teams_msg.addSection(wb_card)
    #add a link to the confluence page
    teams_msg.addLinkButton("Questions?", "https://confluence.teslamotors.com/display/PRODENG/Battery+Module+Hourly+Update")
    teams_msg.send()