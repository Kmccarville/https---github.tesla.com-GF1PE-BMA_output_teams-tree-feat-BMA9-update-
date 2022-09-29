from common.db import db_connector
import common.helper_functions as helper_functions
from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import pymsteams

def get_mamc_starved_table(start_time,end_time):
    #define source tagpaths for each equipment type
    ST10_PATHS = ['[3BM4_30000_Ingress]Project/MDL10/Gripper/Sequences/SeqGripper','[_3BM5_30000_Ingress]Project/MDL10/Gripper/Sequences/SeqGripper']
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
    st10_bma4_percent = round(helper_functions.get_val(st10_df,'3BM4','LINE','Duration')/3600*100,1)
    st10_bma5_percent = round(helper_functions.get_val(st10_df,'3BM5','LINE','Duration')/3600*100,1)
    st20_bma4_percent = round(helper_functions.get_val(st20_df,'3BM4','LINE','Duration')/3600*100,1)
    st20_bma5_percent = round(helper_functions.get_val(st20_df,'3BM5','LINE','Duration')/3600*100,1)
    st30_walk_bma4_percent = round(helper_functions.get_val(st30_walk_df,'3BM4','LINE','Duration')/3600*100,1)
    st30_walk_bma5_percent = round(helper_functions.get_val(st30_walk_df,'3BM5','LINE','Duration')/3600*100,1)
    st30_fix_bma4_percent = round(helper_functions.get_val(st30_fixture_df,'3BM4','LINE','Duration')/3600*100,1)
    st30_fix_bma5_percent = round(helper_functions.get_val(st30_fixture_df,'3BM5','LINE','Duration')/3600*100,1)

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
            <td style="text-align:left"><b>ST10-Bando</b></td>
            <td style="text-align:center">{st10_bma4_percent}%</td>
            <td style="text-align:center">{st10_bma5_percent}%</td>
        </tr>
        """
        # <tr>
        #     <td style="text-align:left"><b>ST20-Frax1</b></td>
        #     <td style="text-align:center">{st20_bma4_percent}%</td>
        #     <td style="text-align:center">{st20_bma5_percent}%</td>
        # </tr>
        # <tr>
        #     <td style="text-align:left"><b>ST30-Bando</b></td>
        #     <td style="text-align:center">{st30_walk_bma4_percent}%</td>
        #     <td style="text-align:center">{st30_walk_bma5_percent}%</td>
        # </tr>
        # <tr>
        #     <td style="text-align:left"><b>ST30-Fixture</b></td>
        #     <td style="text-align:center">{st30_fix_bma4_percent}%</td>
        #     <td style="text-align:center">{st30_fix_bma5_percent}%</td>
        # </tr>

    return html

def get_cta_output(db,start,end):
    query = f"""
            SELECT 
            tp.flowstepname as FLOWSTEP,
            right(a.name,1) as LINE,
            count(distinct tp.thingid) as OUTPUT
            FROM sparq.thingpath tp
            JOIN sparq.actor a on tp.modifiedby = a.id
            WHERE
            tp.flowstepname in ('3BM4-25000','3BM5-25000')
            AND tp.exitcompletioncode = 'PASS'
            AND tp.completed BETWEEN '{start}' AND '{end}'
            GROUP BY 1,2
            """
    df = pd.read_sql(query,db)
    return df

def get_c3a_mamc_output(db,start,end):
    query = f"""
            SELECT 
            tp.flowstepname as FLOWSTEP,
            count(distinct tp.thingid) as OUTPUT
            FROM sparq.thingpath tp
            WHERE
            tp.flowstepname in ('3BM4-34000','3BM5-34000','3BM4-45000','3BM5-45000')
            AND tp.exitcompletioncode = 'PASS'
            AND tp.completed BETWEEN '{start}' AND '{end}'
            GROUP BY 1
            """
    df = pd.read_sql(query,db)
    return df

def output45(env):
    logging.info("output45 start %s" % datetime.utcnow())
    lookback=1 #1 hr
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    df_cta = get_cta_output(mos_con,start,end)
    df_c3a_mamc = get_c3a_mamc_output(mos_con,start,end)
    mos_con.close()

    if len(df_cta):
        df_cta4 = df_cta.query("FLOWSTEP=='3BM4-25000'")
        cta4_1 = round(helper_functions.get_val(df_cta4,1,'LINE','OUTPUT')/28,2)
        cta4_2 = round(helper_functions.get_val(df_cta4,2,'LINE','OUTPUT')/28,2)
        cta4_3 = round(helper_functions.get_val(df_cta4,3,'LINE','OUTPUT')/28,2)
        cta4_4 = round(helper_functions.get_val(df_cta4,4,'LINE','OUTPUT')/28,2)
        cta4_5 = round(helper_functions.get_val(df_cta4,5,'LINE','OUTPUT')/28,2)
        cta4_6 = round(helper_functions.get_val(df_cta4,6,'LINE','OUTPUT')/28,2)
        cta4_7 = round(helper_functions.get_val(df_cta4,7,'LINE','OUTPUT')/28,2)
        cta4_8 = round(helper_functions.get_val(df_cta4,8,'LINE','OUTPUT')/28,2)
        cta4_total = round(df_cta4['OUTPUT'].sum()/28,2) if len(df_cta4) else 0

        df_cta5 = df_cta.query("FLOWSTEP=='3BM5-25000'")
        cta5_1 = round(helper_functions.get_val(df_cta5,1,'LINE','OUTPUT')/28,2)
        cta5_2 = round(helper_functions.get_val(df_cta5,2,'LINE','OUTPUT')/28,2)
        cta5_3 = round(helper_functions.get_val(df_cta5,3,'LINE','OUTPUT')/28,2)
        cta5_4 = round(helper_functions.get_val(df_cta5,4,'LINE','OUTPUT')/28,2)
        cta5_5 = round(helper_functions.get_val(df_cta5,5,'LINE','OUTPUT')/28,2)
        cta5_6 = round(helper_functions.get_val(df_cta5,6,'LINE','OUTPUT')/28,2)
        cta5_7 = round(helper_functions.get_val(df_cta5,7,'LINE','OUTPUT')/28,2)
        cta5_8 = round(helper_functions.get_val(df_cta5,8,'LINE','OUTPUT')/28,2)
        cta5_total = round(df_cta5['OUTPUT'].sum()/28,2) if len(df_cta5) else 0
        cta_total= round(cta4_total+cta5_total,2)
    else:
        cta4_1 = 0
        cta4_2 = 0
        cta4_3 = 0
        cta4_4 = 0
        cta4_5 = 0
        cta4_6 = 0
        cta4_7 = 0
        cta4_8 = 0
        cta5_1 = 0
        cta5_2 = 0
        cta5_3 = 0
        cta5_4 = 0
        cta5_5 = 0
        cta5_6 = 0
        cta5_7 = 0
        cta5_8 = 0
        cta4_total = 0
        cta5_total = 0
        cta_total = 0

    bma4mamc_o = round(helper_functions.get_val(df_c3a_mamc,'3BM4-34000','FLOWSTEP','OUTPUT')/4,2)
    bma5mamc_o = round(helper_functions.get_val(df_c3a_mamc,'3BM5-34000','FLOWSTEP','OUTPUT')/4,2)
    mamc_total = bma4mamc_o+bma5mamc_o
    bma4c3a_o = round(helper_functions.get_val(df_c3a_mamc,'3BM4-45000','FLOWSTEP','OUTPUT')/4,2)
    bma5c3a_o = round(helper_functions.get_val(df_c3a_mamc,'3BM5-45000','FLOWSTEP','OUTPUT')/4,2)
    c3a_total = bma4c3a_o + bma5c3a_o

    uph_html = f"""
                <tr>
                    <th>    </th>
                    <th style="text-align:center">BMA4</th>
                    <th style="text-align:center">BMA5</th>
                    <th style="text-align:center">TOTAL</th>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>CTA</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(cta4_total)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(cta5_total)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(cta_total)}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>MAMC</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(bma4mamc_o)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(bma5mamc_o)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(mamc_total)}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>C3A</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(bma4c3a_o)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(bma5c3a_o)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(c3a_total)}</td>
                </tr>
                <tr bgcolor="#FFFFFF" height=10px></tr>
                <tr>
                    <td>    </td>
                    <td style="text-align:center"><strong>CTA4</strong></td>
                    <td style="text-align:center"><strong>CTA5</strong></td>
                </tr>
                <tr>
                   <td style="text-align:left"><strong>LANE 1</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(cta4_1)}</td>
                    <td style="text-align:center">----</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>LANE 2</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(cta4_2)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(cta5_2)}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>LANE 3</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(cta4_3)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(cta5_3)}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>LANE 4</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(cta4_4)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(cta5_4)}</td>
                </tr>
                <tr>
                   <td style="text-align:left"><strong>LANE 5</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(cta4_5)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(cta5_5)}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>LANE 6</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(cta4_6)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(cta5_6)}</td>
                </tr>
                <tr>
                   <td style="text-align:left"><strong>LANE 7</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(cta4_7)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(cta5_7)}</td>
                </tr>
                <tr>
                    <td style="text-align:left"><strong>LANE 8</strong></td>
                    <td style="text-align:center">{'{:.2f}'.format(cta4_8)}</td>
                    <td style="text-align:center">{'{:.2f}'.format(cta5_8)}</td>
                </tr>
            """

    tsm_html = get_mamc_starved_table(start,end)
    html_payload = '<table>' + uph_html + tsm_html + '</table>'

    webhook_key = 'teams_webhook_BMA45_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']
    
    #making the hourly teams message
    hourly_msg = pymsteams.connectorcard(webhook)
    hourly_msg.title('BMA45 Hourly Update')
    hourly_msg.summary('summary')
    #make a card with the hourly data
    hourly_card = pymsteams.cardsection()
    hourly_card.text(html_payload)
    hourly_msg.addSection(hourly_card)
    hourly_msg.send()