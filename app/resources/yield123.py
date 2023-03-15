import common.helper_functions as helper_functions

from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import pytz
import pymsteams
import warnings


def main(env,eos=False):
    #begin by defining timestamps
    now=datetime.utcnow()
    logging.info("Yield123 start %s" % now)
    lookback=1 #1 hr
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start_time=now_sub1hr.replace(minute=00,second=00,microsecond=00)

    end_time=start_time+timedelta(hours=lookback)
    end_pst=end_time.astimezone(pytz.timezone('US/Pacific'))

    #define global variables
    LINE_LIST = ['3BM1','3BM2','3BM3']
    Parameter_List = [
                    'IC Fail Count',
                    'IC Timeout Count',
                    'NIC Fail Count',
                    'NIC Timeout Count'
                    ]

    #establish db connections
    #mos_con = db_connector(False,"MOS",sql=sql_bma123)
    mos_con = helper_functions.get_sql_conn('mos_rpt2')

    #get the yield of the hour
    yield_df = query_yield(mos_con,start_time,end_time)
    IC_Fail_Count = []
    IC_Timeout_Count = []
    NIC_Fail_Count = []
    NIC_Timeout_Count = []

    for line in LINE_LIST:
        val = get_yieldval(yield_df, line, 'IC Fail Count')*100
        val = round(val) if val >=100 else round(val,2)
        IC_Fail_Count.append(val)

        val = get_yieldval(yield_df, line, 'IC Timeout Count')*100
        val = round(val) if val >=100 else round(val,2)
        IC_Timeout_Count.append(val)

        val = get_yieldval(yield_df, line, 'NIC Fail Count')*100
        val = round(val) if val >=100 else round(val,2)
        NIC_Fail_Count.append(val)

        val = get_yieldval(yield_df, line, 'NIC Timeout Count')*100
        val = round(val) if val >=100 else round(val,2)
        NIC_Timeout_Count.append(val)


 #create bma header
    Yield_Goal = 98.0
    color_str_IC1 = ""
    color_str_IC2 = ""
    color_str_IC3 = ""
    color_str_NIC1 = ""
    color_str_NIC2 = ""
    color_str_NIC3 = ""
    color_str_IC1 = "color:red;" if IC_Fail_Count[0] < Yield_Goal else "font-weight:bold;"
    color_str_IC2 = "color:red;" if IC_Fail_Count[1] < Yield_Goal else "font-weight:bold;"
    color_str_IC3 = "color:red;" if IC_Fail_Count[2] < Yield_Goal else "font-weight:bold;"
    color_str_NIC1 = "color:red;" if NIC_Fail_Count[0] < Yield_Goal else "font-weight:bold;"
    color_str_NIC2 = "color:red;" if NIC_Fail_Count[1] < Yield_Goal else "font-weight:bold;"
    color_str_NIC3 = "color:red;" if NIC_Fail_Count[2] < Yield_Goal else "font-weight:bold;"
    header_html = f"""<tr>
            <th style="text-align:center"></th>
            <th style="text-align:center">BMA1</th>
            <th style="text-align:center">BMA2</th>
            <th style="text-align:center">BMA3</th>
            </tr>
    """

    IC_Fail_Count_html = f"""<tr>
            <td style="text-align:center"><strong>IC Profilometer</strong></td>
            <td style="text-align:center;{color_str_IC1}">{IC_Fail_Count[0]}%</td>
            <td style="text-align:center;{color_str_IC2}">{IC_Fail_Count[1]}%</td>
            <td style="text-align:center;{color_str_IC3}">{IC_Fail_Count[2]}%</td>
            </tr>
    """
    NIC_Fail_Count_html = f"""<tr>
            <td style="text-align:center"><strong>NIC Profilometer</strong></td>
            <td style="text-align:center;{color_str_NIC1}">{NIC_Fail_Count[0]}%</td>
            <td style="text-align:center;{color_str_NIC2}">{NIC_Fail_Count[1]}%</td>
            <td style="text-align:center;{color_str_NIC3}">{NIC_Fail_Count[2]}%</td>
            </tr>
    """
    IC_Timeout_Count_html = f"""<tr>
            <td style="text-align:center"><strong>IC Timeout</strong></td>
            <td style="text-align:center">{IC_Timeout_Count[0]}%</td>
            <td style="text-align:center">{IC_Timeout_Count[1]}%</td>
            <td style="text-align:center">{IC_Timeout_Count[2]}%</td>
            </tr>
    """
    NIC_Timeout_Count_html = f"""<tr>
            <td style="text-align:center"><strong>NIC Timeout</strong></td>
            <td style="text-align:center">{NIC_Timeout_Count[0]}%</td>
            <td style="text-align:center">{NIC_Timeout_Count[1]}%</td>
            <td style="text-align:center">{NIC_Timeout_Count[2]}%</td>
            </tr>
    """

    #create full bma html with the above htmls

    hour_html = '<table>' + "<caption>C3A Dispense Yield Breakdown</caption>" + header_html + IC_Fail_Count_html + NIC_Fail_Count_html + IC_Timeout_Count_html + NIC_Timeout_Count_html + '</table>'

    webhook_key = 'teams_webhook_BMA123_OCAP_Alerts' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']
    
    #making the hourly teams message
    hourly_msg = pymsteams.connectorcard(webhook)
    hourly_msg.title('C3A Yield Hourly Update')
    hourly_msg.summary('summary')
    #make a card with the hourly data
    hourly_card = pymsteams.cardsection()
    hourly_card.text(hour_html)
    hourly_msg.addSection(hourly_card)
    #add a link to the confluence page
    hourly_msg.addLinkButton("Link to OCAP", "https://confluence.teslamotors.com/display/PRODENG/Dispense+-+Out+of+Control+Action+Plan")
    hourly_msg.send()
    
    mos_con.close()



#pull yield
def query_yield(db,start,end):
    yield_query=f"""
   SELECT
    left(actor.name,4) as Line,
    parameter.name as 'PARAMETER',
	Count((CASE WHEN thingdata.valuetext = 0 THEN 1 END)) as Good,
	sum(thingdata.valuetext) as Bad,
	(Count((CASE WHEN thingdata.valuetext = 0 THEN 1 END))/(sum(thingdata.valuetext) + Count((CASE WHEN thingdata.valuetext = 0 THEN 1 END)))) AS Yield
FROM sparq.thingdata
        JOIN sparq.thing ON thing.id = thingdata.thingid
        INNER JOIN sparq.actor ON actor.id = thingdata.actormodifiedby
        INNER JOIN sparq.parameter ON parameter.id = thingdata.parameterid
WHERE
	thingdata.created between '{start}' and '{end}'
	and thingdata.taskid in (select task.id from sparq.task where task.name in ('ClamshellClose001'))
        and thingdata.parameterid in (select parameter.id from sparq.parameter where parameter.name in ('IC Fail Count','NIC Fail Count','IC Timeout Count','NIC Timeout Count'))
	and actor.type = 'EQUIPMENT'
group by parameter.name, actor.name
order by Line
    """
    
    df = pd.read_sql(yield_query,db)
    
    return df

#parse yield dataframe
def get_yieldval(df,line_number,parameter_name):
    if len(df):
        sub_df = df.query(f"Line=='{line_number}' and PARAMETER=='{parameter_name}'")
        val = sub_df.iloc[0]['Yield'] if len(sub_df) else 0
    else:
        val = 0
    return val


#Shiftly-Dispense-Yield 
def get_C3A_Shift_yield_table(start,end):
    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    df = pd.DataFrame({})
    while start < end:
        start_next = start + timedelta(minutes=60)
        query = f"""
          SELECT
                left(actor.name,4) as LINE,
                        (Count((CASE WHEN thingdata.valuetext = 0 THEN 1 END))/(sum(thingdata.valuetext) + Count((CASE WHEN thingdata.valuetext = 0 THEN 1 END))))*100 AS YIELD
                FROM sparq.thingdata
                        JOIN sparq.thing ON thing.id = thingdata.thingid
                        INNER JOIN sparq.actor ON actor.id = thingdata.actormodifiedby
                        INNER JOIN sparq.parameter ON parameter.id = thingdata.parameterid
                WHERE
                        thingdata.created between '{start}' and '{end}'
                        and thingdata.taskid in (select task.id from sparq.task where task.name in ('ClamshellClose001'))
                        and thingdata.parameterid in (select parameter.id from sparq.parameter where parameter.name in ('IC Fail Count','NIC Fail Count','IC Timeout Count','NIC Timeout Count'))
                        and actor.type = 'EQUIPMENT'
                group by actor.name
                order by LINE"""
        df_sub = pd.read_sql(query,mos_con)
        df = pd.concat([df,df_sub],axis=0)
        start += timedelta(minutes=60)

    mos_con.close()

    bma1_dispense_yield = round(helper_functions.get_val(df,'3BM1','LINE','YIELD'),1)
    bma2_dispense_yield = round(helper_functions.get_val(df,'3BM2','LINE','YIELD'),1)
    bma3_dispense_yield = round(helper_functions.get_val(df,'3BM3','LINE','YIELD'),1)

    html=f"""
    <tr>
        <td style="text-align:center"><b>C3A Dispense Yield</b></td>
        <td style="text-align:center">{bma1_dispense_yield}%</td>
        <td style="text-align:center">{bma2_dispense_yield}%</td>
        <td style="text-align:center">{bma3_dispense_yield}%</td>
    </tr>
    """
    return html