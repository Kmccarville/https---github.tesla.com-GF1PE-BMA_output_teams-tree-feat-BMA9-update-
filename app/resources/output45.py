from common.db import db_connector
import common.helper_functions as helper_functions
from datetime import datetime
from datetime import timedelta
import logging

def get_starved_table(start_time,end_time):
    #define source tagpaths for each equipment type
    ST10_PATHS = ['[3BM4_30000_Ingress]Project/MDL10/Gripper/Sequences/SeqGripper','[_3BM5_31000_CouplerStripInstall]Project/MDL01/TSM/StateControl']
    ST20_PATHS = ['[3BM4_31000_CouplerStripInstall]Project/MDL10/EM Seq/StripGripper/EM_Sequence','[_3BM5_31000_CouplerStripInstall]Project/MDL10/EM Seq/StripGripper/EM_Sequence']
    ST30_WALK_PATHS = ['[3BM4_33000_ModuleClose_NewVersion]Project/MDL00/State Machine Summary/MDL01/StateControl','[_3BM5_33000_ModuleClose_NewVersion]Project/MDL00/State Machine Summary/MDL01/StateControl']
    ST30_FIXTURE_PATHS = ['[3BM4_33000_ModuleClose_NewVersion]Project/MDL00/State Machine Summary/MDL10/StateControl','[_3BM5_33000_ModuleClose_NewVersion]Project/MDL00/State Machine Summary/MDL10/StateControl']
    
    plc_con = helper_functions.get_sql_conn('plc_db')

    #get starveddata for each tagpath set
    st10_df = helper_functions.query_tsm_state(plc_con,start_time, end_time, ST10_PATHS, 'Starved')
    st20_df = helper_functions.query_tsm_state(plc_con,start_time, end_time, ST20_PATHS, 'Starved')
    st30_walk_df = helper_functions.query_tsm_state(plc_con,start_time, end_time, ST30_WALK_PATHS, 'Starved')
    st30_fixture_df = helper_functions.query_tsm_state(plc_con,start_time, end_time, ST30_FIXTURE_PATHS, 'Starved')

    #get starve percentage (divide by 3600s and multiply by 100%)
    st10_bma4_percent = round(helper_functions.get_val(st10_df,'3BM4')/3600*100,1)
    st10_bma5_percent = round(helper_functions.get_val(st10_df,'3BM5')/3600*100,1)
    st20_bma4_percent = round(helper_functions.get_val(st20_df,'3BM4')/3600*100,1)
    st20_bma5_percent = round(helper_functions.get_val(st20_df,'3BM5')/3600*100,1)
    st30_walk_bma4_percent = round(helper_functions.get_val(st30_walk_df,'3BM4')/3600*100,1)
    st30_walk_bma5_percent = round(helper_functions.get_val(st30_walk_df,'3BM5')/3600*100,1)
    st30_fix_bma4_percent = round(helper_functions.get_val(st30_fixture_df,'3BM4')/3600*100,1)
    st30_fix_bma5_percent = round(helper_functions.get_val(st30_fixture_df,'3BM5')/3600*100,1)

    html=f"""
        <tr bgcolor="#FFFFFF" height=10px></tr>
        <tr>
            <td colspan="3" style="text-align:center"><b>STARVATION %</b></td>
        </tr>
        <tr>
            <td>    </td>
            <td style="text-align:center"><strong>MAMC4</strong></td>
            <td style="text-align:center"><strong>MAMC5</strong></td>
        </tr>
        <tr>
            <td style="text-align:left"><b>STA10-Bando</b></td>
            <td style="text-align:center">{st10_bma4_percent}%</td>
            <td style="text-align:center">{st10_bma5_percent}%</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>STA20-Frax1</b></td>
            <td style="text-align:center">{st20_bma4_percent}%</td>
            <td style="text-align:center">{st20_bma5_percent}%</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>STA30-Bando</b></td>
            <td style="text-align:center">{st30_walk_bma4_percent}%</td>
            <td style="text-align:center">{st30_walk_bma5_percent}%</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>STA30-Fixture</b></td>
            <td style="text-align:center">{st30_fix_bma4_percent}%</td>
            <td style="text-align:center">{st30_fix_bma5_percent}%</td>
        </tr>
        """
    return html

def output45(env):
    logging.info("output45 start %s" % datetime.utcnow())
    lookback=1 #1 hr
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)
    
    #Grab BMA4-CTA hourly data
    sql_bma4cta=helper_functions.file_reader("resources/sql_queries/bma4cta_output.sql")
    sql_bma4cta=sql_bma4cta.format(start_time=start,end_time=end)
    df_bma4cta=db_connector(False,"MOS",sql=sql_bma4cta)
    df_bma4cta.fillna(0)
    if len(df_bma4cta):
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-01'")
        CTA4_1 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-02'")
        CTA4_2 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-03'")
        CTA4_3 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-04'")
        CTA4_4 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-05'")
        CTA4_5 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-06'")
        CTA4_6 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-07'")
        CTA4_7 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma4cta.query("LINE=='3BM4-20000-08'")
        CTA4_8 = sub_df.iloc[0][1] if len(sub_df) else 0
        CTA4_SUM=round(df_bma4cta['UPH'].sum(), 2)
    else:
        CTA4_1 = 0
        CTA4_2 = 0
        CTA4_3 = 0
        CTA4_4 = 0
        CTA4_5 = 0
        CTA4_6 = 0
        CTA4_7 = 0
        CTA4_8 = 0
        CTA4_SUM = 0
    logging.info("bma4cta end %s" % datetime.utcnow())

    #Grab BMA5-CTA hourly data
    sql_bma5cta=helper_functions.file_reader("resources/sql_queries/bma5cta_output.sql")
    sql_bma5cta=sql_bma5cta.format(start_time=start,end_time=end)
    df_bma5cta=db_connector(False,"MOS",sql=sql_bma5cta)
    df_bma5cta.fillna(0)
    if len(df_bma5cta):
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-02'")
        CTA5_2 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-03'")
        CTA5_3 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-04'")
        CTA5_4 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-05'")
        CTA5_5 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-06'")
        CTA5_6 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-07'")
        CTA5_7 = sub_df.iloc[0][1] if len(sub_df) else 0
        sub_df = df_bma5cta.query("LINE=='3BM5-20000-08'")
        CTA5_8 = sub_df.iloc[0][1] if len(sub_df) else 0
        CTA5_SUM=round(df_bma5cta['UPH'].sum(), 2)
    else:
        CTA5_2 = 0
        CTA5_3 = 0
        CTA5_4 = 0
        CTA5_5 = 0
        CTA5_6 = 0
        CTA5_7 = 0
        CTA5_8 = 0
        CTA5_SUM = 0
    logging.info("bam5cta end %s" % datetime.utcnow())

    #Grab BMA4-MAMC hourly data
    sql_bma4mamc= helper_functions.file_reader("resources/sql_queries/bma4mamc_output.sql")
    sql_bma4mamc=sql_bma4mamc.format(start_time=start,end_time=end)
    df_bma4mamc=db_connector(False,"MOS",sql=sql_bma4mamc)
    df_bma4mamc.fillna(0)
    bma4mamc_o=df_bma4mamc['count(distinct tp.thingid)/4'][0]
    logging.info("bma4mamc end %s" % datetime.utcnow())

    #Grab BMA5-MAMC hourly data
    sql_bma5mamc=helper_functions.file_reader("resources/sql_queries/bma5mamc_output.sql")
    sql_bma5mamc=sql_bma5mamc.format(start_time=start,end_time=end)
    df_bma5mamc=db_connector(False,"MOS",sql=sql_bma5mamc)
    df_bma5mamc.fillna(0)
    bma5mamc_o=df_bma5mamc['count(distinct tp.thingid)/4'][0]
    logging.info("bma5mamc_o end %s" % datetime.utcnow())

    #Grab BMA4-C3A hourly data
    sql_bma4c3a=helper_functions.file_reader("resources/sql_queries/bma4c3a_output.sql")
    sql_bma4c3a=sql_bma4c3a.format(start_time=start,end_time=end)
    df_bma4c3a=db_connector(False,"MOS",sql=sql_bma4c3a)
    df_bma4c3a.fillna(0)
    bma4c3a_o=df_bma4c3a['count(distinct tp.thingid)/4'][0]
    logging.info("bma4c3a_o end %s" % datetime.utcnow())

    #Grab BMA5-C3A hourly data
    sql_bma5c3a=helper_functions.file_reader("resources/sql_queries/bma5c3a_output.sql")
    sql_bma5c3a=sql_bma5c3a.format(start_time=start,end_time=end)
    df_bma5c3a=db_connector(False,"MOS",sql=sql_bma5c3a)
    df_bma5c3a.fillna(0)
    bma5c3a_o=df_bma5c3a['count(distinct tp.thingid)/4'][0]
    logging.info("bma5c3a end %s" % datetime.utcnow())

    #Set outputs
    CTA_TOTAL= round(CTA4_SUM+CTA5_SUM,2)

    MAMC4 = bma4mamc_o
    MAMC5 = bma5mamc_o
    MAMC_TOTAL= MAMC4+MAMC5

    C3A4 = bma4c3a_o
    C3A5 = bma5c3a_o
    C3A_TOTAL = C3A4+C3A5

    uph_html = f"""
                <tr>
                    <th>    </th>
                    <th style="text-align:center">BMA4</th>
                    <th style="text-align:center">BMA5</th>
                    <th style="text-align:center">TOTAL</th>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>CTA</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_SUM)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_SUM)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA_TOTAL)}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>MAMC</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(MAMC4)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(MAMC5)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(MAMC_TOTAL)}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>C3A</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(C3A4)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(C3A5)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(C3A_TOTAL)}</td>
                </tr>
                <tr bgcolor="#FFFFFF" height=10px></tr>
                <tr>
                    <td>    </td>
                    <td style="text-align:center"><strong>CTA4</strong></td>
                    <td style="text-align:center"><strong>CTA5</strong></td>
                </tr>
                <tr>
                   <td style="text-align:left"><strong>LANE 1</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_1)}</td>
                    <td style="text-align:center">----</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>LANE 2</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_2)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_2)}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>LANE 3</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_3)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_3)}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>LANE 4</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_4)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_4)}</td>
                </tr>
                <tr>
                   <td style="text-align:left"><strong>LANE 5</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_5)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_5)}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>LANE 6</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_6)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_6)}</td>
                </tr>
                <tr>
                   <td style="text-align:left"><strong>LANE 7</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_7)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_7)}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>LANE 8</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA4_8)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(CTA5_8)}</td>
                </tr>
            """

    tsm_html = get_starved_table(start,end)
    html_payload = '<table>' + uph_html + tsm_html + '</table>'

    #post to BMA45-PE --> Output Channel
    if env=="prod":
        helper_functions.send_to_teams('teams_webhook_BMA45_Updates', 'BMA45 Hourly Update', html_payload,retry=1)
    else:
        helper_functions.send_to_teams('teams_webhook_DEV_Updates','BMA45 Hourly Update', html_payload)

