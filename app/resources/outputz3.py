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
    
def get_mttr_df(env,eos,db,start,end):
    bonder_start = start+timedelta(hours=-2)
    BONDTOOL_FAULT_CODE = 214
    bonders = query_bonder_list(db)
    bonder_logs_df = query_bonder_logs(db,bonder_start,end)

    all_times_df = pd.DataFrame({})
    for bonder in bonders['MACHINE_ID']:   
        #get subset of bonder logs
        sub_df = bonder_logs_df.query(f"MACHINE_ID=='{bonder}'")
        bondtool_change = 0
        bt_change_times = []
        in_auto_times = []
        for row in sub_df.itertuples(True,'Tuples'):
            #BOND TOOL CHANGE LOGIC
            if row.FAULT_CODE == BONDTOOL_FAULT_CODE and row.BONDER_MODE == 0:
                bondtool_change+=1
                if bondtool_change == 1:
                    bt_change_times.append(row.START_DATE_TIME)
            if row.BONDER_MODE==1 and bondtool_change>0:
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
                    Time_Stamp BETWEEN convert_tz('{row.BT_CHANGE_TIME}','GMT','US/Pacific') and (convert_tz('{row.IN_AUTO_TIME}','GMT','US/Pacific') + interval 60 second)
                    AND EquipmentID = '{row.MACHINE_ID}'
                    AND Source = 'Ready To Running'
                ORDER BY ID DESC
                LIMIT 1
                """
        df = pd.read_sql(query,db)
        bt_df = pd.concat([bt_df,df],axis=0)

    if len(bt_df):
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
        #prep insert for database logging only on prod branch to avoid duplicate inserts
        if env=='prod' and not eos and len(bt_df):
            try:
                bt_df_insert = bt_df[['MACHINE_ID','BT_START_TIME','BT_COMPLETE_TIME','BT','WG','CB','FT','SS','IDEAL_SEC']]
                bt_df_insert.rename({
                                    'BT_START_TIME' : 'START_TIME',
                                    'BT_COMPLETE_TIME' : 'COMPLETE_TIME',
                                    'BT' : 'BONDTOOL_CHANGE',
                                    'WG' : 'WIREGUIDE_CHANGE',
                                    'CB' : 'CUTTERBLADE_CHANGE',
                                    'FT' : 'FEEDTUBE_CHANGE',
                                    'SS' : 'SETSCREW_CHANGE',
                                    'IDEAL_SEC' : 'IDEAL_CHANGE_SEC'
                                    },inplace=True,axis=1)

                bt_df_insert.loc[:,'START_TIME'] = bt_df_insert.apply(lambda x: helper_functions.convert_from_utc_to_pst(x.START_TIME),axis=1)
                bt_df_insert.loc[:,'COMPLETE_TIME'] = bt_df_insert.apply(lambda x: helper_functions.convert_from_utc_to_pst(x.COMPLETE_TIME),axis=1)
                ict_con = helper_functions.get_sql_conn('interconnect_eng')
                bt_df_insert.to_sql('consumable_change_log',ict_con,'m3_teep_v3',if_exists='append',index=False)
                logging.info('Successfully Inserted Consumable Logs')
                ict_con.close()
            except:
                logging.info('Failed to Insert Consumable Logs')

        #filter out all set screw changes
        bt_df = bt_df.query("SS==0")

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
        return bt_summary

    else:
        empty_df = pd.DataFrame({})
        return empty_df

def mttr_to_html(df):

    quad1a_target = round(helper_functions.get_val_2(df,1,'LINE','A','QUAD','IDEAL_MIN'),1)
    quad1b_target = round(helper_functions.get_val_2(df,1,'LINE','B','QUAD','IDEAL_MIN'),1)
    quad1c_target = round(helper_functions.get_val_2(df,1,'LINE','C','QUAD','IDEAL_MIN'),1)
    quad1d_target = round(helper_functions.get_val_2(df,1,'LINE','D','QUAD','IDEAL_MIN'),1)

    quad2a_target = round(helper_functions.get_val_2(df,2,'LINE','A','QUAD','IDEAL_MIN'),1)
    quad2b_target = round(helper_functions.get_val_2(df,2,'LINE','B','QUAD','IDEAL_MIN'),1)
    quad2c_target = round(helper_functions.get_val_2(df,2,'LINE','C','QUAD','IDEAL_MIN'),1)
    quad2d_target = round(helper_functions.get_val_2(df,2,'LINE','D','QUAD','IDEAL_MIN'),1)
    
    quad3a_target = round(helper_functions.get_val_2(df,3,'LINE','A','QUAD','IDEAL_MIN'),1)
    quad3b_target = round(helper_functions.get_val_2(df,3,'LINE','B','QUAD','IDEAL_MIN'),1)
    quad3c_target = round(helper_functions.get_val_2(df,3,'LINE','C','QUAD','IDEAL_MIN'),1)
    quad3d_target = round(helper_functions.get_val_2(df,3,'LINE','D','QUAD','IDEAL_MIN'),1)

    quad4a_target = round(helper_functions.get_val_2(df,4,'LINE','A','QUAD','IDEAL_MIN'),1)
    quad4b_target = round(helper_functions.get_val_2(df,4,'LINE','B','QUAD','IDEAL_MIN'),1)
    quad4c_target = round(helper_functions.get_val_2(df,4,'LINE','C','QUAD','IDEAL_MIN'),1)

    quad5a_target = round(helper_functions.get_val_2(df,5,'LINE','A','QUAD','IDEAL_MIN'),1)
    quad5b_target = round(helper_functions.get_val_2(df,5,'LINE','B','QUAD','IDEAL_MIN'),1)

    #
    quad1a_actual = round(helper_functions.get_val_2(df,1,'LINE','A','QUAD','ACTUAL_MIN'),1)
    quad1b_actual = round(helper_functions.get_val_2(df,1,'LINE','B','QUAD','ACTUAL_MIN'),1)
    quad1c_actual = round(helper_functions.get_val_2(df,1,'LINE','C','QUAD','ACTUAL_MIN'),1)
    quad1d_actual = round(helper_functions.get_val_2(df,1,'LINE','D','QUAD','ACTUAL_MIN'),1)

    quad2a_actual = round(helper_functions.get_val_2(df,2,'LINE','A','QUAD','ACTUAL_MIN'),1)
    quad2b_actual = round(helper_functions.get_val_2(df,2,'LINE','B','QUAD','ACTUAL_MIN'),1)
    quad2c_actual = round(helper_functions.get_val_2(df,2,'LINE','C','QUAD','ACTUAL_MIN'),1)
    quad2d_actual = round(helper_functions.get_val_2(df,2,'LINE','D','QUAD','ACTUAL_MIN'),1)
    
    quad3a_actual = round(helper_functions.get_val_2(df,3,'LINE','A','QUAD','ACTUAL_MIN'),1)
    quad3b_actual = round(helper_functions.get_val_2(df,3,'LINE','B','QUAD','ACTUAL_MIN'),1)
    quad3c_actual = round(helper_functions.get_val_2(df,3,'LINE','C','QUAD','ACTUAL_MIN'),1)
    quad3d_actual = round(helper_functions.get_val_2(df,3,'LINE','D','QUAD','ACTUAL_MIN'),1)

    quad4a_actual = round(helper_functions.get_val_2(df,4,'LINE','A','QUAD','ACTUAL_MIN'),1)
    quad4b_actual = round(helper_functions.get_val_2(df,4,'LINE','B','QUAD','ACTUAL_MIN'),1)
    quad4c_actual = round(helper_functions.get_val_2(df,4,'LINE','C','QUAD','ACTUAL_MIN'),1)

    quad5a_actual = round(helper_functions.get_val_2(df,5,'LINE','A','QUAD','ACTUAL_MIN'),1)
    quad5b_actual = round(helper_functions.get_val_2(df,5,'LINE','B','QUAD','ACTUAL_MIN'),1)

    html=f"""
        <tr>
            <td></td>
            <th colspan="2" style="text-align:center">3BM1</th>
            <th colspan="2" style="text-align:center">3BM2</th>
            <th colspan="2" style="text-align:center">3BM3</th>
            <th colspan="2" style="text-align:center">3BM4</th>
            <th colspan="2" style="text-align:center">3BM5</th>
            <th></th>
        </tr>
        <tr>
            <td></td>
            <th style="text-align:center"><strong>Actual</strong></th>
            <th style="text-align:center;"><strong>Target</strong></th>
            <th style="text-align:center"><strong>Actual</strong></th>
            <th style="text-align:center;"><strong>Target</strong></th>
            <th style="text-align:center"><strong>Actual</strong></th>
            <th style="text-align:center;"><strong>Target</strong></th>
            <th style="text-align:center"><strong>Actual</strong></th>
            <th style="text-align:center;"><strong>Target</strong></th>
            <th style="text-align:center"><strong>Actual</strong></th>
            <th style="text-align:center"><strong>Target</strong></th>
        </tr>
        <tr>
            <td style="text-align:left"><b>A</b></td>
            <td style="text-align:center;{'color:red' if quad1a_actual > quad1a_target else ''}">{'---' if quad1a_actual==0 else quad1a_actual}</td>
            <td style="text-align:center;">{'---' if quad1a_target==0 else quad1a_target}</td>
            <td style="text-align:center;{'color:red' if quad2a_actual > quad2a_target else ''}">{'---' if quad2a_actual==0 else quad2a_actual}</td>
            <td style="text-align:center;">{'---' if quad2a_target==0 else quad2a_target}</td>
            <td style="text-align:center;{'color:red' if quad3a_actual > quad3a_target else ''}">{'---' if quad3a_actual==0 else quad3a_actual}</td>
            <td style="text-align:center;">{'---' if quad3a_target==0 else quad3a_target}</td>
            <td style="text-align:center;{'color:red' if quad4a_actual > quad4a_target else ''}">{'---' if quad4a_actual==0 else quad4a_actual}</td>
            <td style="text-align:center;">{'---' if quad4a_target==0 else quad4a_target}</td>
            <td style="text-align:center;{'color:red' if quad5a_actual > quad5a_target else ''}">{'---' if quad5a_actual==0 else quad5a_actual}</td>
            <td style="text-align:center">{'---' if quad5a_target==0 else quad5a_target}</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>B</b></td>
            <td style="text-align:center;{'color:red' if quad1b_actual > quad1b_target else ''}">{'---' if quad1b_actual==0 else quad1b_actual}</td>
            <td style="text-align:center;">{'---' if quad1b_target==0 else quad1b_target}</td>
            <td style="text-align:center;{'color:red' if quad2b_actual > quad2b_target else ''}">{'---' if quad2b_actual==0 else quad2b_actual}</td>
            <td style="text-align:center;">{'---' if quad2b_target==0 else quad2b_target}</td>
            <td style="text-align:center;{'color:red' if quad3b_actual > quad3b_target else ''}">{'---' if quad3b_actual==0 else quad3b_actual}</td>
            <td style="text-align:center;">{'---' if quad3b_target==0 else quad3b_target}</td>
            <td style="text-align:center;{'color:red' if quad4b_actual > quad4b_target else ''}">{'---' if quad4b_actual==0 else quad4b_actual}</td>
            <td style="text-align:center;">{'---' if quad4b_target==0 else quad4b_target}</td>
            <td style="text-align:center;{'color:red' if quad5b_actual > quad5b_target else ''}">{'---' if quad5b_actual==0 else quad5b_actual}</td>
            <td style="text-align:center;">{'---' if quad5b_target==0 else quad5b_target}</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>C</b></td>
            <td style="text-align:center;{'color:red' if quad1c_actual > quad1c_target else ''}">{'---' if quad1c_actual==0 else quad1c_actual}</td>
            <td style="text-align:center;">{'---' if quad1c_target==0 else quad1c_target}</td>
            <td style="text-align:center;{'color:red' if quad2c_actual > quad2c_target else ''}">{'---' if quad2c_actual==0 else quad2c_actual}</td>
            <td style="text-align:center;">{'---' if quad2c_target==0 else quad2c_target}</td>
            <td style="text-align:center;{'color:red' if quad3c_actual > quad3c_target else ''}">{'---' if quad3c_actual==0 else quad3c_actual}</td>
            <td style="text-align:center;">{'---' if quad3c_target==0 else quad3c_target}</td>
            <td style="text-align:center;{'color:red' if quad4c_actual > quad4c_target else ''}">{'---' if quad4c_actual==0 else quad4c_actual}</td>
            <td style="text-align:center;">{'---' if quad4c_target==0 else quad4c_target}</td>
            <td style="text-align:center;">---</td>
            <td style="text-align:center">---</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>D</b></td>
            <td style="text-align:center;{'color:red' if quad1d_actual > quad1d_target else ''}">{'---' if quad1d_actual==0 else quad1d_actual}</td>
            <td style="text-align:center;">{'---' if quad1d_target==0 else quad1d_target}</td>
            <td style="text-align:center;{'color:red' if quad2d_actual > quad2d_target else ''}">{'---' if quad2d_actual==0 else quad2d_actual}</td>
            <td style="text-align:center;">{'---' if quad2d_target==0 else quad2d_target}</td>
            <td style="text-align:center;{'color:red' if quad3d_actual > quad3d_target else ''}">{'---' if quad3d_actual==0 else quad3d_actual}</td>
            <td style="text-align:center;">{'---' if quad3d_target==0 else quad3d_target}</td>
            <td style="text-align:center;">---</td>
            <td style="text-align:center;">---</td>
            <td style="text-align:center;">---</td>
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
    ing4_starved = round(helper_functions.get_val(ing_df,'3BM4','LINE','Duration')/seconds_between*100/3,1)
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
            <td style="text-align:center">{ing1_starved}%</td>
            <td style="text-align:center">{ing2_starved}%</td>
            <td style="text-align:center">{ing3_starved}%</td>
            <td style="text-align:center">{ing4_starved}%</td>
            <td style="text-align:center">{ing5_starved}%</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>PO MTRs</b></td>
            <td style="text-align:center">{po1_starved}%</td>
            <td style="text-align:center">{po2_starved}%</td>
            <td style="text-align:center">{po3_starved}%</td>
            <td style="text-align:center">{po4_starved}%</td>
            <td style="text-align:center">{po5_starved}%</td>
        </tr>
        """
    return html

def get_empty_bonders(db,tagpath):
    query = f"""
            SELECT sqlth54.intvalue
            FROM rno_ia_taghistory_batterymodule.sqlth_54_data sqlth54
            LEFT JOIN rno_ia_taghistory_batterymodule.sqlth_te te ON sqlth54.tagid = te.id
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
    ict_con = helper_functions.get_sql_conn('interconnect_eng')

    df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)
    starve_table = get_starved_table(plc_con,start,end)
    mttr_df = get_mttr_df(env,eos,ict_con,start,end)

    #get empty bonder spots
    L1_EMPTY_TAGPATH = '02/_Summary/line1free'
    L2_EMPTY_TAGPATH = '02/_Summary/line2free'
    L3_EMPTY_TAGPATH = '02/_Summary/line3free'
    L4_EMPTY_TAGPATH = '02/_Summary/line4free'
    L5_EMPTY_TAGPATH = '02/_Summary/line5free'
    l1_empty_spots = get_empty_bonders(plc_con,L1_EMPTY_TAGPATH)
    l2_empty_spots = get_empty_bonders(plc_con,L2_EMPTY_TAGPATH)
    l3_empty_spots = get_empty_bonders(plc_con,L3_EMPTY_TAGPATH)
    l4_empty_spots = get_empty_bonders(plc_con,L4_EMPTY_TAGPATH)
    l5_empty_spots = get_empty_bonders(plc_con,L5_EMPTY_TAGPATH)

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
                <tr>
                    <td style="text-align:left"><b>Empty Bonders</b></td>
                    <td style="text-align:center">{l1_empty_spots}</td>
                    <td style="text-align:center">{l2_empty_spots}</td>
                    <td style="text-align:center">{l3_empty_spots}</td>
                    <td style="text-align:center">{l4_empty_spots}</td>
                    <td style="text-align:center">{l5_empty_spots}</td>
                    <td style="text-align:center"><b>---</b></td>
                </tr>
                """


    output_html = "<table>" + "<caption>Throughput</caption>" + output_table + "</table>"
    starved_html = "<table>" + "<caption>Starvation %</caption>" + starve_table + "</table>"

    if len(mttr_df):
        mttr_table = mttr_to_html(mttr_df)
        wb_html = "<table>" + "<caption>Consumable Change Overtime (minutes)</caption>" +  mttr_table + "</table>"
    else:
        wb_html = "No Consumables Changed This Hour"

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
