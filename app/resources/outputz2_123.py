from common import helper_functions
from resources import yield123

from datetime import datetime
from datetime import timedelta
import logging
import numpy as np
import pandas as pd
import pymsteams

def get_mamc_yield_table(start,end):
    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    df = pd.DataFrame({})
    while start < end:
        start_next = start + timedelta(minutes=60)
        query = f"""
                SELECT
                left(a.name,4) as LINE,
                ((COUNT(DISTINCT tp.thingid) - COUNT(DISTINCT nc.thingid)) / COUNT(DISTINCT tp.thingid)* 100) AS YIELD
                FROM
                    sparq.thingpath tp
                    JOIN sparq.actor a ON a.id = tp.actorcreatedby
                    LEFT JOIN
                    sparq.nc ON nc.thingid = tp.thingid
                WHERE
                    tp.flowstepname IN ('3BM-29500')
                    AND tp.completed BETWEEN '{start}' AND '{start_next}'
                GROUP BY 1
                """
        df_sub = pd.read_sql(query,mos_con)
        df = pd.concat([df,df_sub],axis=0)
        start += timedelta(minutes=60)

    mos_con.close()

    bma1_yield = round(helper_functions.get_val(df,'3BM1','LINE','YIELD'),1)
    bma2_yield = round(helper_functions.get_val(df,'3BM2','LINE','YIELD'),1)
    bma3_yield = round(helper_functions.get_val(df,'3BM3','LINE','YIELD'),1)

    html=f"""
    <tr>
        <td style="text-align:center"><b>MAMC Yield</b></td>
        <td style="text-align:center">{bma1_yield}%</td>
        <td style="text-align:center">{bma2_yield}%</td>
        <td style="text-align:center">{bma3_yield}%</td>
    </tr>
    """
    return html

#Shiftly-Dispense-Yield 
def get_c3a_yield_table(start,end):
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

def get_performance_table(start,end):

    plc_con = helper_functions.get_sql_conn('plc_db')
    seconds_between = (end - start).seconds
    AUTO_CLOSER_PATHS = [
                        '[3BM1_29500_01]Line1A/Closer/TSM/StateControl',
                        '[3BM2_29500_01]LINE1A/Closer/TSM/StateControl',
                        '[3BM3_29500_01]LINE1A/Closer/TSM/StateControl'
                        ]

    auto_close_df = helper_functions.query_tsm_state(plc_con,start, end, AUTO_CLOSER_PATHS, 'Starved')
    #get percentage (divide by seconds in between start and end and multiply by 100%)
    auto_close_bma1_percent = round(helper_functions.get_val(auto_close_df,'3BM1','LINE','Duration')/seconds_between*100,1)
    auto_close_bma2_percent = round(helper_functions.get_val(auto_close_df,'3BM2','LINE','Duration')/seconds_between*100,1)
    auto_close_bma3_percent = round(helper_functions.get_val(auto_close_df,'3BM3','LINE','Duration')/seconds_between*100,1)
    
    BANDO_CT_PATHS = [
                        '[3BM1_29500_01]BandoLandCT/CycleTimeReporting/PalletInfeed',
                        '[3BM2_29500_01]BandoLandCT/CycleTimeReporting/PalletInfeed',
                        '[3BM3_29500_01]BandoLandCT/CycleTimeReporting/PalletInfeed'
                        ]

    BANDO_LOW_LIMIT = 36
    BANDO_HIGH_LIMIT = 95

    bando_df = helper_functions.query_tsm_cycle_time(plc_con,start,end,BANDO_CT_PATHS,BANDO_LOW_LIMIT,BANDO_HIGH_LIMIT)
    bando_ct_bma1 = round(helper_functions.get_val(bando_df,'3BM1','LINE','CT_SEC'),1)
    bando_ct_bma2 = round(helper_functions.get_val(bando_df,'3BM2','LINE','CT_SEC'),1)
    bando_ct_bma3 = round(helper_functions.get_val(bando_df,'3BM3','LINE','CT_SEC'),1)

    SIDEMOUNT_CT_PATHS = [
                        '[3BM1_29500_01]ManualStationReporting/SidemountInstall/StateControl',
                        '[3BM2_29500_01]ManualStationReporting/SidemountInstall/StateControl',
                        '[3BM3_29500_01]ManualStationReporting/SidemountInstall/StateControl'
                        ]

    SIDEMOUNT_LOW_LIMIT = 38
    SIDEMOUNT_HIGH_LIMIT = 165

    sidemount_df = helper_functions.query_tsm_cycle_time(plc_con,start,end,SIDEMOUNT_CT_PATHS,SIDEMOUNT_LOW_LIMIT,SIDEMOUNT_HIGH_LIMIT)
    sidemount_ct_bma1 = round(helper_functions.get_val(sidemount_df,'3BM1','LINE','CT_SEC'),1)
    sidemount_ct_bma2 = round(helper_functions.get_val(sidemount_df,'3BM2','LINE','CT_SEC'),1)
    sidemount_ct_bma3 = round(helper_functions.get_val(sidemount_df,'3BM3','LINE','CT_SEC'),1)
    
    QIS_CT_PATHS = [
                        '[3BM1_29500_01]ManualStationReporting/QIS/StateControl',
                        '[3BM2_29500_01]ManualStationReporting/QIS/StateControl',
                        '[3BM3_29500_01]ManualStationReporting/QIS/StateControl'
                        ]

    QIS_LOW_LIMIT = 41
    QIS_HIGH_LIMIT = 98

    qis_df = helper_functions.query_tsm_cycle_time(plc_con,start,end,QIS_CT_PATHS,QIS_LOW_LIMIT,QIS_HIGH_LIMIT)
    qis_ct_bma1 = round(helper_functions.get_val(qis_df,'3BM1','LINE','CT_SEC'),1)
    qis_ct_bma2 = round(helper_functions.get_val(qis_df,'3BM2','LINE','CT_SEC'),1)
    qis_ct_bma3 = round(helper_functions.get_val(qis_df,'3BM3','LINE','CT_SEC'),1)

    plc_con.close()
    
    html=f"""
        <tr>
            <th style="text-align:left">Cycle Time</th>
            <th style="text-align:left">(Target:</th>
            <th style="text-align:left"> 67s)</th>
            <th></th>
            <th></th>
            <th style="text-align:left">Starved %</th>
        </tr>
        <tr>
            <td></td>
            <th style="text-align:center"><strong>BMA1</strong></th>
            <th style="text-align:center"><strong>BMA2</strong></th>
            <th style="text-align:center"><strong>BMA3</strong></th>
            <td></td>
            <td></td>
            <th style="text-align:center"><strong>BMA1</strong></th>
            <th style="text-align:center"><strong>BMA2</strong></th>
            <th style="text-align:center"><strong>BMA3</strong></th>
        </tr>
        <tr>
            <td style="text-align:left"><b>BandoLand</b></td>
            <td style="text-align:center">{bando_ct_bma1}</td>
            <td style="text-align:center">{bando_ct_bma2}</td>
            <td style="text-align:center">{bando_ct_bma3}</td>
            <td>||</td>
            <td style="text-align:right"><b>Auto Closer</b></td>
            <td style="text-align:center">{auto_close_bma1_percent}%</td>
            <td style="text-align:center">{auto_close_bma2_percent}%</td>
            <td style="text-align:center">{auto_close_bma3_percent}%</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>Sidemount</b></td>
            <td style="text-align:center">{sidemount_ct_bma1}</td>
            <td style="text-align:center">{sidemount_ct_bma2}</td>
            <td style="text-align:center">{sidemount_ct_bma3}</td>
            <td>||</td>
        </tr>
        <tr>
            <td style="text-align:left"><b>QIS</b></td>
            <td style="text-align:center">{qis_ct_bma1}</td>
            <td style="text-align:center">{qis_ct_bma2}</td>
            <td style="text-align:center">{qis_ct_bma3}</td>
            <td>||</td>
        </tr>
        """
    
    return html

def mamc_records(lookback,mamc1,mamc2,mamc3,webhook):
    logging.info(f'Starting {lookback} hour ACTA records')
    # check for output records for 1 hour
    record_con = helper_functions.get_sql_conn('pedb',schema='records')

    mamc1 = mamc1
    mamc2 = mamc2
    mamc3 = mamc3
    mamc123 = mamc1 + mamc2 + mamc3
    names = ['MAMC123']
    carsets = [mamc123]
    newRecordArray = []
    prevShiftArray = []
    prevDateArray = []
    prevRecordArray = []

    for line in range(len(names)):
        newRecord, prevShift, prevDate, prevRecord = helper_functions.evaluate_record(record_con,names[line],lookback,carsets[line])
        newRecordArray.append(newRecord)
        prevShiftArray.append(prevShift)
        prevDateArray.append(prevDate)
        prevRecordArray.append(prevRecord)

    record_con.close()

    if True in np.isin(True,newRecordArray):
        for line in range(len(newRecordArray)):
            if newRecordArray[line] == True:
                lineName = names[line]
                carsPrev = prevRecordArray[line]
                shiftPrev = prevShiftArray[line]
                datePrev = prevDateArray[line]
                carsNew = carsets[line]
                shiftNow,date = helper_functions.get_shift_and_date()

                logging.info(f'Starting Record Post for {lineName}')

                html = f"""
                        <tr>
                            <th style="text-align:center"><strong></strong></th>
                            <th style="text-align:center"><strong>Shift</strong></th>
                            <th style="text-align:center"><strong>Date</strong></th>
                            <th style="text-align:center"><strong>Carsets</strong></th>
                        </tr>
                        <tr>
                            <td style="text-align:right"><b>Prev Record</b></td>
                            <td style="text-align:center">{shiftPrev}</td>
                            <td style="text-align:center">{datePrev}</td>
                            <td style="text-align:center">{carsPrev}</td>
                        </tr>
                        <tr>
                            <td style="text-align:right"><b>NEW RECORD</b></td>
                            <td style="text-align:center">{shiftNow}</td>
                            <td style="text-align:center">{date}</td>
                            <td style="text-align:center">{carsNew:.1f}</td>
                        </tr>
                        """
                output_html = '<table>' + html + '</table>'

                #making the record teams message
                record_msg = pymsteams.connectorcard(webhook)
                title = f'NEW RECORD ACHEIVED FOR {lineName} | {lookback} HOUR'
                record_msg.title(title)
                record_msg.summary('summary')
                TESLA_RED = '#cc0000'
                msg_color = TESLA_RED
                record_msg.color(msg_color)
                #make a card with the hourly data
                recordCard = pymsteams.cardsection()
                recordCard.text(output_html)

                record_msg.addSection(recordCard)
                #SEND IT
                try:
                    record_msg.send()
                except pymsteams.TeamsWebhookException:
                    logging.warn("Webhook timed out, retry once")
                    try:
                        record_msg.send()
                    except pymsteams.TeamsWebhookException:
                        logging.exception("Webhook timed out twice -- pass to next area")
    else:
        logging.info('No Record to post about')


def ac3a_records(lookback,c3a1,c3a2,c3a3,webhook):
    logging.info(f'Starting {lookback} hour ACTA records')
    # check for output records for 1 hour
    record_con = helper_functions.get_sql_conn('pedb',schema='records')

    c3a1 = c3a1
    c3a2 = c3a2
    c3a3 = c3a3
    line123 = c3a1 + c3a2 + c3a3
    names = ['AC3A123']
    carsets = [line123]
    newRecordArray = []
    prevShiftArray = []
    prevDateArray = []
    prevRecordArray = []

    for line in range(len(names)):
        newRecord, prevShift, prevDate, prevRecord = helper_functions.evaluate_record(record_con,names[line],lookback,carsets[line])
        newRecordArray.append(newRecord)
        prevShiftArray.append(prevShift)
        prevDateArray.append(prevDate)
        prevRecordArray.append(prevRecord)

    record_con.close()

    if True in np.isin(True,newRecordArray):
        for line in range(len(newRecordArray)):
            if newRecordArray[line] == True:
                lineName = names[line]
                carsPrev = prevRecordArray[line]
                shiftPrev = prevShiftArray[line]
                datePrev = prevDateArray[line]
                carsNew = carsets[line]
                shiftNow,date = helper_functions.get_shift_and_date()

                logging.info(f'Starting Record Post for {lineName}')

                html = f"""
                        <tr>
                            <th style="text-align:center"><strong></strong></th>
                            <th style="text-align:center"><strong>Shift</strong></th>
                            <th style="text-align:center"><strong>Date</strong></th>
                            <th style="text-align:center"><strong>Carsets</strong></th>
                        </tr>
                        <tr>
                            <td style="text-align:right"><b>Prev Record</b></td>
                            <td style="text-align:center">{shiftPrev}</td>
                            <td style="text-align:center">{datePrev}</td>
                            <td style="text-align:center">{carsPrev}</td>
                        </tr>
                        <tr>
                            <td style="text-align:right"><b>NEW RECORD</b></td>
                            <td style="text-align:center">{shiftNow}</td>
                            <td style="text-align:center">{date}</td>
                            <td style="text-align:center">{carsNew:.1f}</td>
                        </tr>
                        """
                output_html = '<table>' + html + '</table>'

                #making the record teams message
                record_msg = pymsteams.connectorcard(webhook)
                title = f'NEW RECORD ACHEIVED FOR {lineName} | {lookback} HOUR'
                record_msg.title(title)
                record_msg.summary('summary')
                TESLA_RED = '#cc0000'
                msg_color = TESLA_RED
                record_msg.color(msg_color)
                #make a card with the hourly data
                recordCard = pymsteams.cardsection()
                recordCard.text(output_html)

                record_msg.addSection(recordCard)
                #SEND IT
                try:
                    record_msg.send()
                except pymsteams.TeamsWebhookException:
                    logging.warn("Webhook timed out, retry once")
                    try:
                        record_msg.send()
                    except pymsteams.TeamsWebhookException:
                        logging.exception("Webhook timed out twice -- pass to next area")
    else:
        logging.info('No Record to post about')


def main(env,eos=False):
    #define start and end time for the hour
    lookback=12 if eos else 1
    now=datetime.utcnow()
    logging.info("Output Z2 123 start %s" % datetime.utcnow())
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    logging.info(str(start))
    logging.info(str(end))

    #define globals
    NORMAL_DIVISOR = 4
    MAMC_295_FLOWSTEP= '3BM-29500'
    MAMC_296_FLOWSTEP= '3BM-29600'
    C3A_FLOWSTEP = '3BM-40001'
    LINES = ['3BM1','3BM2','3BM3']

    #create flowstep list
    flowsteps = [MAMC_295_FLOWSTEP,MAMC_296_FLOWSTEP,C3A_FLOWSTEP]
    #create mos connection
    mos_con = helper_functions.get_sql_conn('mos_rpt2')
    #get output for flowsteps
    df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)
    
    hourly_goal_dict = helper_functions.get_zone_line_goals(zone=2)

    mos_con.close()

    mamc_295_outputs = []
    mamc_296_outputs = []
    c3a_outputs = []
    for line in LINES:
        mamc_295_outputs.append(helper_functions.get_output_val(df_output,MAMC_295_FLOWSTEP,line))
        mamc_296_outputs.append(helper_functions.get_output_val(df_output,MAMC_296_FLOWSTEP,line))
        c3a_outputs.append(helper_functions.get_output_val(df_output,C3A_FLOWSTEP,line))

    mamc_outputs = np.add(mamc_295_outputs, mamc_296_outputs)

    total_mamc_output = helper_functions.get_output_val(df_output,MAMC_295_FLOWSTEP) + helper_functions.get_output_val(df_output,MAMC_296_FLOWSTEP)
    total_c3a_output = helper_functions.get_output_val(df_output,C3A_FLOWSTEP)

    #create bma header
    bma_header_html = f"""<tr>
            <th style="text-align:center"></th>
            <th style="text-align:center">BMA1</th>
            <th style="text-align:center">BMA2</th>
            <th style="text-align:center">BMA3</th>
            <th style="text-align:center">TOTAL</th>
            </tr>
            """
    #create mamc output row
    mamc_output_html = f"""<tr>
            <td style="text-align:center"><strong>MAMC</strong></td>
            <td style="text-align:center">{mamc_outputs[0]/NORMAL_DIVISOR:.2f}</td>
            <td style="text-align:center">{mamc_outputs[1]/NORMAL_DIVISOR:.2f}</td>
            <td style="text-align:center">{mamc_outputs[2]/NORMAL_DIVISOR:.2f}</td>
            <td style="text-align:center"><strong>{total_mamc_output/NORMAL_DIVISOR:.2f}</strong></td>
            """
    #create c3a output row
    c3a_output_html = f"""<tr>
            <td style="text-align:center"><strong>C3A</strong></td>
            <td style="text-align:center">{c3a_outputs[0]/NORMAL_DIVISOR:.2f}</td>
            <td style="text-align:center">{c3a_outputs[1]/NORMAL_DIVISOR:.2f}</td>
            <td style="text-align:center">{c3a_outputs[2]/NORMAL_DIVISOR:.2f}</td>
            <td style="text-align:center"><strong>{total_c3a_output/NORMAL_DIVISOR:.2f}</strong></td>
            """
    
    z2_goal_html = f"""<tr>
        <td style="text-align:center"><strong>GOAL</strong></td>
        <td style="text-align:center">{int(hourly_goal_dict['3BM1'])}</td>
        <td style="text-align:center">{int(hourly_goal_dict['3BM2'])}</td>
        <td style="text-align:center">{int(hourly_goal_dict['3BM3'])}</td>
        """

    #create full bma html with the above htmls
    bma_html = '<table>' + "<caption>Throughput</caption>" + bma_header_html + mamc_output_html + c3a_output_html + z2_goal_html+ '</table>'

    #get cycle time html
    header_html = """
                    <tr>
                    <td></td>
                    <th style="text-align:center"><strong>BMA1</strong></th>
                    <th style="text-align:center"><strong>BMA2</strong></th>
                    <th style="text-align:center"><strong>BMA3</strong></th>
                    </tr>
                """
    # starved_table = get_starved_table(start,end)
    # cycle_time_table = get_cycle_time_table(start,end)
    performance_table = get_performance_table(start,end)
    mamc_yield_table = get_mamc_yield_table(start,end)
    if eos:
        c3a_yield_table = get_c3a_yield_table(start,end)

    cycle_time_html = '<table>' + "<caption>Performance</caption>"  + performance_table + '</table>'

    if eos:
        z2_yield_html = '<table>' + "<caption>Yield</caption>" + header_html + mamc_yield_table + c3a_yield_table + '</table>'
    else:
        z2_yield_html = '<table>' + "<caption>Yield</caption>" + header_html + mamc_yield_table + '</table>'

    #get webhook based on environment
    webhook_key = 'teams_webhook_BMA123_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']

    #start end of shift message
    teams_msg = pymsteams.connectorcard(webhook)
    title = 'BMA123 ZONE2 EOS Report' if eos else 'BMA123 ZONE2 Hourly Update'
    teams_msg.title(title)
    teams_msg.summary('summary')
    K8S_BLUE = '#3970e4'
    TESLA_RED = '#cc0000'
    msg_color = TESLA_RED if eos else K8S_BLUE
    teams_msg.color(msg_color)
    
    #create cards for each major html
    bma_card = pymsteams.cardsection()
    bma_card.text(bma_html)
    teams_msg.addSection(bma_card)

    cycle_card = pymsteams.cardsection()
    cycle_card.text(cycle_time_html)
    yield_card = pymsteams.cardsection()
    yield_card.text(z2_yield_html)

    teams_msg.addSection(cycle_card)
    teams_msg.addSection(yield_card)

    teams_msg.addLinkButton("Questions?", "https://confluence.teslamotors.com/display/PRODENG/Battery+Module+Hourly+Update")
    #SEND IT
    try:
        teams_msg.send()
    except pymsteams.TeamsWebhookException:
        logging.warn("Webhook timed out, retry once")
        try:
            teams_msg.send()
        except pymsteams.TeamsWebhookException:
            logging.exception("Webhook timed out twice -- pass to next area")

    # do mamc records MAMC123 1 hour ONLY for now
    if lookback == 1:
        mamc1 = mamc_outputs[0]/NORMAL_DIVISOR
        mamc2 = mamc_outputs[1]/NORMAL_DIVISOR
        mamc3 = mamc_outputs[2]/NORMAL_DIVISOR
        webhook_key = 'teams_webhook_Zone2_123_Records' if env=='prod' else 'teams_webhook_DEV_Updates'
        webhook_json = helper_functions.get_pw_json(webhook_key)
        webhook = webhook_json['url']
        mamc_records(lookback,mamc1,mamc2,mamc3,webhook)

    # do records for AC3A 1 12 24 hour only for now
    line1 = c3a_outputs[0]/NORMAL_DIVISOR
    line2 = c3a_outputs[1]/NORMAL_DIVISOR
    line3 = c3a_outputs[2]/NORMAL_DIVISOR
    webhook_key = 'teams_webhook_Zone2_123_Records' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']
    ac3a_records(lookback,line1,line2,line3,webhook)
