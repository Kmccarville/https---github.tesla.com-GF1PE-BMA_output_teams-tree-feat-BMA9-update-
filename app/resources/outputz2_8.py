import logging
import traceback
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pymsteams
from common import helper_functions
from common.constants import K8S_BLUE, TESLA_RED


def get_mamc_ncs(db,start,end):
    query=f"""
        select count(distinct nc.thingid) as NCs
        from thingpath tp
        inner join nc on nc.thingid = tp.thingid
        where tp.completed between '{start}' and '{end}'
        and tp.flowstepid in ('1038276','1038270','1038275','1038277','1038274','1038271','1019245','1019264')
        and nc.detectedatstepid in ('277978','277974','277976','277979')
        and tp.iscurrent = 0
    """
    df = pd.read_sql(query,db)
    if len(df) > 0:
        ncs = df.get_value(0,'NCs')
    else:
        ncs = 0
    return ncs

def get_mamc_ncs_table(db,start,end):
    query=f"""
        select nc.description as NCGroup, count(distinct nc.thingid) as NCs
        from sparq.thingpath tp
        inner join sparq.nc on nc.thingid = tp.thingid
        where tp.completed between '{start}' and '{end}'
        and tp.flowstepid in ('1038276','1038270','1038275','1038277','1038274','1038271','1019245','1019264')
        and nc.detectedatstepid in ('277978','277974','277976','277979')
        and tp.iscurrent = 0
        group by 1
        order by 2 desc
    """
    df = pd.read_sql(query,db)
    ncs = df['NCs'].sum()
    ncs_table = df.to_html(index=False,justify='center')
    return ncs_table, ncs

def bma8_records(lookback,c3a8,webhook):
    logging.info(f'Starting {lookback} hour ACTA records')
    # check for output records for 1 hour
    record_con = helper_functions.get_sql_conn('pedb',schema='records')

    c3a8 = c3a8
    names = ['C3A8']
    carsets = [c3a8]
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
    logging.info("Output Z2 8 start %s" % datetime.utcnow())
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)

    logging.info(str(start))
    logging.info(str(end))

    #define globals
    NORMAL_DIVISOR = 4
    MAMC_FLOWSTEP = '3BM8-29500'
    MAMC_FLOWSTEP2 = '3BM8-29600'
    MAMC_LINE = '3BM8'
    C3A_FLOWSTEP = '3BM8-44000'
    C3A_LINE = '3BM8'

    #create flowstep list
    flowsteps = [MAMC_FLOWSTEP,C3A_FLOWSTEP,MAMC_FLOWSTEP2]
    #create mos connection
    mos_con = helper_functions.get_sql_conn('mos_rpt2',schema='sparq')
    #get output for flowsteps
    df_output = helper_functions.get_flowstep_outputs(mos_con,start,end,flowsteps)


    mamc_output_good1 = helper_functions.get_output_val(df_output, MAMC_FLOWSTEP,MAMC_LINE)
    mamc_output_good2 = helper_functions.get_output_val(df_output, MAMC_FLOWSTEP2,MAMC_LINE)
    mamc_output_good = mamc_output_good1 + mamc_output_good2

    mamc_output_ncs = get_mamc_ncs(mos_con,start, end)
    c3a_outputs = helper_functions.get_output_val(df_output,C3A_FLOWSTEP,C3A_LINE)
    
    NC_Table_html, num_ncs = get_mamc_ncs_table(mos_con,start,end)
    
    hourly_goal_dict = helper_functions.get_zone_line_goals(zone=2,hours=lookback)

    mos_con.close()

    #create bma header
    bma_header_html = f"""<tr>
            <th style="text-align:center"></th>
            <th style="text-align:left">UPH</th>
            </tr>
    """
    #create mamc output row
    mamc_output_html = f"""<tr>
            <td style="text-align:center"><strong>MAMC</strong></td>
            <td style="text-align:left">{mamc_output_good/NORMAL_DIVISOR:.2f} (+ {mamc_output_ncs/NORMAL_DIVISOR:.2f} NCs)</td>
            </tr>
    """
    #create c3a output row
    c3a_output_html = f"""<tr>
            <td style="text-align:center"><strong>C3A</strong></td>
            <td style="text-align:left">{c3a_outputs/NORMAL_DIVISOR:.2f}</td>
            </tr>
    """
    
    goal_html = f"""<tr>
            <td style="text-align:center"><strong>GOAL</strong></td>
            <td style="text-align:left">{int(hourly_goal_dict['3BM8'])}</td>
            </tr>
    """

    #create full bma html with the above htmls
    output_html = '<table>' + bma_header_html + mamc_output_html + c3a_output_html + NC_Table_html + '</table>'

    if env == 'prod':
        teams_con = helper_functions.get_sql_conn('pedb', schema='teams_output')
        try:
            historize_to_db(teams_con,
                            mamc_output_good,
                            c3a_outputs,
                            num_ncs,
                            NORMAL_DIVISOR)
        except Exception as e:
            logging.exception(f'Historization for z2_8 failed. See: {e}')
        teams_con.close()
        
    #get webhook based on environment
    webhook_key = 'teams_webhook_BMA8_Updates' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']

    #start end of shift message
    teams_msg = pymsteams.connectorcard(webhook)
    title = 'BMA8 EOS Report' if eos else 'BMA8 Hourly Update'
    teams_msg.title(title)
    teams_msg.summary('summary')
    msg_color = TESLA_RED if eos else K8S_BLUE
    teams_msg.color(msg_color)
    teams_msg.printme()
    print(teams_msg)

    #create cards for each major html
    output_card = pymsteams.cardsection()
    output_card.text(output_html)
    teams_msg.addSection(output_card)

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

    # do records for C3A8 1 12 24 hour only for now
    c3a8 = c3a_outputs/NORMAL_DIVISOR
    webhook_key = 'teams_webhook_BMA8_Records' if env=='prod' else 'teams_webhook_DEV_Updates'
    webhook_json = helper_functions.get_pw_json(webhook_key)
    webhook = webhook_json['url']
    bma8_records(lookback,c3a8,webhook)

def historize_to_db(db, mamc, c3a, num_ncs, NORMAL_DIVISOR):
    curr_date = datetime.now().date()
    fdate = curr_date.strftime('%Y-%m-%d')
    hour = datetime.now().hour
    df_insert = pd.DataFrame({
        'mamc' : [round(mamc/NORMAL_DIVISOR, 2) if mamc is not None else None],
        'c3a' : [round(c3a/NORMAL_DIVISOR, 2) if c3a is not None else None],
        'num_ncs' : [num_ncs if num_ncs is not None else None],
        'hour': [hour],
        'date': [fdate]
    }, index=['line'])
    
    df_insert.to_sql('zone2_bma8', con=db, if_exists='append', index=False)