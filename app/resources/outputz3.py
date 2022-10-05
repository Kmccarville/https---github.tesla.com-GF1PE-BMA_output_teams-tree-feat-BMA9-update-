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
    total_output = 0
    for line in LINES:
        header_html += f"""<th style="text-align:center">{line}</th>"""
        output_val = helper_functions.get_output_val(df_output,line,PO_FLOWSTEP)/4
        total_output += output_val
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
        wb_card.text(wb_html)
        teams_msg.addSection(wb_card)
    #add a link to the confluence page
    teams_msg.addLinkButton("Questions?", "https://confluence.teslamotors.com/display/PRODENG/Hourly+Update")
    teams_msg.send()