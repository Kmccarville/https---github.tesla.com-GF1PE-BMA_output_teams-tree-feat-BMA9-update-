#Auto Posting for Milan output - trigger around 7 PM everyday
from common import helper_functions 
import pandas as pd
from urllib.parse import quote
import sqlalchemy
import pymysql
import pymsteams
from datetime import datetime
from datetime import timedelta

def sendTeamsMessage(webhook, title, summary, message,color='#cc0000'):
    teams_msg = pymsteams.connectorcard(webhook)
    teams_msg.title(title)
    teams_msg.summary(summary)
    teams_msg.color(color)

    # create cards for each major html
    output_card = pymsteams.cardsection()
    output_card.text(message)
    teams_msg.addSection(output_card)

    # SEND text to Teams
    teams_msg.send()

def main(env, eos=False):
    lookback=12 if eos
    now=datetime.utcnow()
    now_sub1hr=now+timedelta(hours=-lookback)
    start=now_sub1hr.replace(minute=00,second=00,microsecond=00)
    end=start+timedelta(hours=lookback)
    
    query = f"""SELECT 
    t.name AS 'Thing_Name',
    t.description AS 'Description',
    tp.flowstepname 'Flowstep Name',
    t.state AS 'State',
    CASE
        WHEN tp.flowstepname = 'ModuleNCM-11100' THEN 'Reconfigure Module'
        WHEN tp.flowstepname = 'ModuleNCM-14000' THEN 'Terminal Replace'
        WHEN tp.flowstepname = 'ModuleNCM-14100' THEN 'Offline Terminal Weld'
        WHEN tp.flowstepname = 'ModuleNCM-14200' THEN 'Reseat OR Bend Terminal'
        WHEN tp.flowstepname = 'ModuleNCM-14300' THEN 'Replace Insulator Tape'
        WHEN tp.flowstepname = 'ModuleNCM-14309' THEN 'Remove VSH'
        WHEN tp.flowstepname = 'ModuleNCM-14400' THEN 'Replace VSH'
        WHEN tp.flowstepname = 'ModuleNCM-14500' THEN 'Offline VSH Weld'
        WHEN tp.flowstepname = 'ModuleNCM-14600' THEN 'Replace BMB'
        WHEN tp.flowstepname = 'ModuleNCM-14700' THEN 'Offline BMB Bond'
        WHEN tp.flowstepname = 'ModuleNCM-14701' THEN 'Inline BMB Bond'
        WHEN tp.flowstepname = 'ModuleNCM-14800' THEN 'Root Cause BMB'
        WHEN tp.flowstepname = 'ModuleNCM-15500' THEN 'State Of Charge'
        WHEN tp.flowstepname = 'ModuleNCM-15600' THEN 'Uprev'
        WHEN tp.flowstepname = 'ModuleNCM-15700' THEN 'Reprint Module Serial'
        WHEN tp.flowstepname = 'ModuleNCM-15800' THEN 'Reprint Config OR China Serial'
        WHEN tp.flowstepname = 'ModuleNCM-16400' THEN 'Manually Start Config'
        WHEN tp.flowstepname = 'ModuleNCM-16700' THEN 'Misroutes OR Genealogy'
        WHEN tp.flowstepname = 'ModuleNCM-16800' THEN 'Potting Seepage'
        WHEN tp.flowstepname = 'ModuleNCM-16900' THEN 'CC Delamination'
        WHEN tp.flowstepname = 'ModuleNCM-17000' THEN 'NIC Wetout Scrap REVIEW'
        WHEN tp.flowstepname = 'ModuleNCM-17200' THEN 'Manual Build'
        WHEN tp.flowstepname = 'ModuleNCM-17300' THEN 'Hipot Teardown'
        WHEN tp.flowstepname = 'ModuleNCM-14900' THEN 'Hipot DCW'
        WHEN tp.flowstepname = 'ModuleNCM-14901' THEN 'Hipot ACW'
        WHEN tp.flowstepname = 'ModuleNCM-12600' THEN 'Hold Process'
        WHEN tp.flowstepname = 'ModuleNCM-12700' THEN 'Hold Quality'
        WHEN tp.flowstepname = 'ModuleNCM-12900' THEN 'Hold NPI'
        WHEN tp.flowstepname = 'ModuleNCM-15000' THEN 'Hold Potting24h'
        WHEN tp.flowstepname = 'ModuleNCM-15100' THEN 'Hold Potting20m'
        WHEN tp.flowstepname = 'ModuleNCM-17100' THEN 'Hold Inventory'
        WHEN tp.flowstepname = 'ModuleNCM-11000' THEN 'Cell Cap'
        WHEN tp.flowstepname = 'ModuleNCM-11200' THEN 'Cell Plug'
        WHEN tp.flowstepname = 'ModuleNCM-11300' THEN 'Bond Redo'
        WHEN tp.flowstepname = 'ModuleNCM-11500' THEN 'Adhesive Remove'
        WHEN tp.flowstepname = 'ModuleNCM-11600' THEN 'Barb Damage'
        WHEN tp.flowstepname = 'ModuleNCM-11700' THEN 'Tombstone Remove'
        WHEN tp.flowstepname = 'ModuleNCM-11800' THEN 'Falcon Cap'
        WHEN tp.flowstepname = 'ModuleNCM-11900' THEN 'Falcon Prosthetic'
        WHEN tp.flowstepname = 'ModuleNCM-12100' THEN 'YDatum Prosthetic'
        WHEN tp.flowstepname = 'ModuleNCM-12400' THEN 'Clamshell Patch'
        WHEN tp.flowstepname = 'ModuleNCM-12500' THEN 'Current Collector Patch'
        WHEN tp.flowstepname = 'ModuleNCM-13600' THEN 'Sidemount Rework Other'
        WHEN tp.flowstepname = 'ModuleNCM-13700' THEN 'FOD Remove'
        WHEN tp.flowstepname = 'ModuleNCM-13800' THEN 'IC Clamshell Gap'
        WHEN tp.flowstepname = 'ModuleNCM-13900' THEN 'Sidemount Foot'
        WHEN tp.flowstepname = 'ModuleNCM-16200' THEN 'Cap Gap'
        WHEN tp.flowstepname = 'ModuleNCM-16600' THEN 'Coupler Gap'
        WHEN tp.flowstepname = 'ModuleNCM-17400' THEN 'End Cell'
        WHEN tp.flowstepname = 'ModuleNCM-17500' THEN 'Spur Defect Check-In'
        WHEN tp.flowstepname = 'ModuleNCM-17600' THEN 'Pack Return Leak Test'
        WHEN tp.flowstepname = 'ScrapREVIEW' THEN 'Scrap REVIEW'
        WHEN tp.flowstepname = 'ModuleNCM-12200' THEN 'Potted CC Patch'
        WHEN tp.flowstepname = 'ModuleNCM-16500' THEN 'New RepairTrial1'
        WHEN tp.flowstepname = 'ModuleNCM-16100' THEN 'Use As Is'
        WHEN tp.flowstepname = 'ModuleNCM-15200' THEN 'Add Potting'
        WHEN tp.flowstepname = 'ModuleNCM-15300' THEN 'Remove Potting From Sidemounts'
        WHEN tp.flowstepname = 'ModuleNCM-16300' THEN 'Potted ReBond'
    END 'Rework Description'
FROM
    thingpath tp
        INNER JOIN
    thing t ON t.id = tp.thingid
WHERE
    tp.flowstepname IN ('ModuleNCM-11100' , 'ModuleNCM-14000',
        'ModuleNCM-14100',
        'ModuleNCM-14200',
        'ModuleNCM-14300',
        'ModuleNCM-14309',
        'ModuleNCM-14400',
        'ModuleNCM-14500',
        'ModuleNCM-14600',
        'ModuleNCM-14700',
        'ModuleNCM-14701',
        'ModuleNCM-14800',
        'ModuleNCM-15500',
        'ModuleNCM-15600',
        'ModuleNCM-15700',
        'ModuleNCM-15800',
        'ModuleNCM-16400',
        'ModuleNCM-16700',
        'ModuleNCM-16800',
        'ModuleNCM-16900',
        'ModuleNCM-17000',
        'ModuleNCM-17200',
        'ModuleNCM-17300',
        'ModuleNCM-14900',
        'ModuleNCM-14901',
        'ModuleNCM-12600',
        'ModuleNCM-12700',
        'ModuleNCM-12900')
    AND tp.stepstatus = 'Exited'
    AND tp.exited > NOW() - INTERVAL {lookback} HOUR"""
        
    db = helper_functions.get_sql_conn('mos_rpt2',schema="sparq")
    df = pd.read_sql(query,db)
    db.close()
    
    Total_Counts = len(df)
    Reconfigure_Module = (df['Rework Description'] == 'Reconfigure Module').sum()
    Terminal_Replace = (df['Rework Description'] == 'Terminal Replace').sum()
    Offline_Terminal_Weld = (df['Rework Description'] == 'Offline Terminal Weld').sum()
    Reseat_OR_BendTerminal = (df['Rework Description'] == 'Reseat OR Bend Terminal').sum()
    Replace_Insulator_Tape = (df['Rework Description'] == 'Replace Insulator Tape').sum()
    Remove_VSH = (df['Rework Description'] == 'Remove VSH').sum()
    Replace_VSH = (df['Rework Description'] == 'Replace VSH').sum()
    Offline_VSH_Weld = (df['Rework Description'] == 'Offline VSH Weld').sum()
    Replace_BMB = (df['Rework Description'] == 'Replace BMB').sum()
    Offline_BMB_Bond = (df['Rework Description'] == 'Offline BMB Bond').sum()
    Inline_BMB_Bond = (df['Rework Description'] == 'Inline BMB Bond').sum()
    Root_Cause_BMB = (df['Rework Description'] == 'Root Cause BMB').sum()
    State_Of_Charge = (df['Rework Description'] == 'State Of Charge').sum()
    Uprev = (df['Rework Description'] == 'Uprev').sum()
    Reprint_Module_Serial = (df['Rework Description'] == 'Reprint Module Serial').sum()
    Reprint_Config_OR_China_Serial = (df['Rework Description'] == 'Reprint Config OR China Serial').sum()
    Manually_Start_Config = (df['Rework Description'] == 'Manually Start Config').sum()
    Misroutes_OR_Genealogy = (df['Rework Description'] == 'Misroutes OR Genealogy').sum()
    Potting_Seepage = (df['Rework Description'] == 'Potting Seepage').sum()
    CC_Delamination = (df['Rework Description'] == 'CC Delamination').sum()
    NIC_Wetout_Scrap_REVIEW = (df['Rework Description'] == 'NIC Wetout Scrap REVIEW').sum()
    Manual_Build = (df['Rework Description'] == 'Manual Build').sum()
    Hipot_Teardown = (df['Rework Description'] == 'Hipot Teardown').sum()
    Hipot_DCW = (df['Rework Description'] == 'Hipot DCW').sum()
    Hipot_ACW = (df['Rework Description'] == 'Hipot ACW').sum()
    Hold_Process = (df['Rework Description'] == 'Hold Process').sum()
    Hold_Quality = (df['Rework Description'] == 'Hold Quality').sum()
    Hold_NPI = (df['Rework Description'] == 'Hold NPI').sum()
    Hold_Potting24h = (df['Rework Description'] == 'Hold Potting24h').sum()
    Hold_Potting20m = (df['Rework Description'] == 'Hold Potting20m').sum()
    Hold_Inventory = (df['Rework Description'] == 'Hold Inventory').sum()
    Cell_Cap = (df['Rework Description'] == 'Cell Cap').sum()
    Cell_Plug = (df['Rework Description'] == 'Cell Plug').sum()
    Bond_Redo = (df['Rework Description'] == 'Bond Redo').sum()
    Adhesive_Remove = (df['Rework Description'] == 'Adhesive Remove').sum()
    Barb_Damage = (df['Rework Description'] == 'Barb Damage').sum()
    Tombstone_Remove = (df['Rework Description'] == 'Tombstone Remove').sum()
    Falcon_Cap = (df['Rework Description'] == 'Falcon Cap').sum()
    Falcon_Prosthetic = (df['Rework Description'] == 'Falcon Prosthetic').sum()
    YDatum_Prosthetic = (df['Rework Description'] == 'YDatum Prosthetic').sum()
    Clamshell_Patch = (df['Rework Description'] == 'Clamshell Patch').sum()
    Current_Collector_Patch = (df['Rework Description'] == 'Current Collector Patch').sum()
    Sidemount_Rework_Other = (df['Rework Description'] == 'Sidemount Rework Other').sum()
    FOD_Remove = (df['Rework Description'] == 'FOD Remove').sum()
    IC_Clamshell_Gap = (df['Rework Description'] == 'IC Clamshell Gap').sum()
    Sidemount_Foot = (df['Rework Description'] == 'Sidemount Foot').sum()
    Cap_Gap = (df['Rework Description'] == 'Cap Gap').sum()
    Coupler_Gap = (df['Rework Description'] == 'Coupler Gap').sum()
    End_Cell = (df['Rework Description'] == 'End Cell').sum()
    Spur_Defect_Check_In = (df['Rework Description'] == 'Spur Defect Check-In').sum()
    Pack_Return_Leak_Test = (df['Rework Description'] == 'Pack Return Leak Test').sum()
    Scrap_REVIEW = (df['Rework Description'] == 'Scrap REVIEW').sum()
    Potted_CC_Patch = (df['Rework Description'] == 'Potted CC Patch').sum()
    New_RepairTrial1 = (df['Rework Description'] == 'New RepairTrial1').sum()
    Use_As_Is = (df['Rework Description'] == 'Use As Is').sum()
    Add_Potting = (df['Rework Description'] == 'Add Potting ').sum()
    Remove_Potting_From_Sidemounts = (df['Rework Description'] == 'Remove Potting From Sidemounts').sum()
    Potted_ReBond = (df['Rework Description'] == 'Potted ReBond').sum()

    message = f"""<html>
    <head>
    <style>
    table, th, td {{
      border: 5px solid black;
      border-collapse: collapse;
    }}
    </style>
    </head>
    <body>

    <table>
                <tr>
                    <th style="text-align:center">Rework Process Name</th>
                    <th style="text-align:center">Count of Repairs</th>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Total Repairs</strong></td>
                    <td <td style="text-align:center"><strong>{Total_Counts}</strong></td>
                </tr>
                <tr>                        
                    <td <td style="text-align:left"><strong>Reconfigure Module</strong></td>
                    <td <td style="text-align:center">{Reconfigure_Module}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Terminal Replace</strong></td>
                    <td <td style="text-align:center">{Terminal_Replace}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Offline Terminal  Weld</strong></td>
                    <td <td style="text-align:center">{Offline_Terminal_Weld}</td>           
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Reseat OR Bend Terminal</strong></td>
                    <td <td style="text-align:center">{Reseat_OR_BendTerminal}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Replace Insulator Tape</strong></td>
                    <td <td style="text-align:center">{Replace_Insulator_Tape}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Remove VSH</strong></td>
                    <td <td style="text-align:center">{Remove_VSH}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Replace VSH</strong></td>
                    <td <td style="text-align:center">{Replace_VSH}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Offline VSH Weld</strong></td>
                    <td <td style="text-align:center">{Offline_VSH_Weld}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Replace BMB</strong></td>
                    <td <td style="text-align:center">{Replace_BMB}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Offline BMB Bond</strong></td>
                    <td <td style="text-align:center">{Offline_BMB_Bond}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Inline BMB Bond</strong></td>
                    <td <td style="text-align:center">{Inline_BMB_Bond}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Root Cause BMB</strong></td>
                    <td <td style="text-align:center">{Root_Cause_BMB}</td>
                </tr>            
                <tr>
                    <td <td style="text-align:left"><strong>State Of Charge</strong></td>
                    <td <td style="text-align:center">{State_Of_Charge}</td>
                </tr>            
                <tr>
                    <td <td style="text-align:left"><strong>Uprev</strong></td>
                    <td <td style="text-align:center">{Uprev}</td>
                </tr>            
                <tr>
                    <td <td style="text-align:left"><strong>Reprint Module Serial</strong></td>
                    <td <td style="text-align:center">{Reprint_Module_Serial}</td>
                </tr>            
                <tr>
                    <td <td style="text-align:left"><strong>Reprint Config OR China Serial</strong></td>
                    <td <td style="text-align:center">{Reprint_Config_OR_China_Serial}</td>
                </tr>            
                <tr>
                    <td <td style="text-align:left"><strong>Manually Start Config </strong></td>
                    <td <td style="text-align:center">{Manually_Start_Config}</td>
                </tr>            
                <tr>
                    <td <td style="text-align:left"><strong>Misroutes OR Genealogy</strong></td>
                    <td <td style="text-align:center">{Misroutes_OR_Genealogy}</td>
                </tr>            
                <tr>
                    <td <td style="text-align:left"><strong>Potting Seepage</strong></td>
                    <td <td style="text-align:center">{Potting_Seepage}</td>
                </tr>            
                <tr>
                    <td <td style="text-align:left"><strong>CC Delamination</strong></td>
                    <td <td style="text-align:center">{CC_Delamination}</td>
                </tr>            
                <tr>
                    <td <td style="text-align:left"><strong>NIC Wetout Scrap REVIEW</strong></td>
                    <td <td style="text-align:center">{NIC_Wetout_Scrap_REVIEW}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Manual Build</strong></td>
                    <td <td style="text-align:center">{Manual_Build}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Hipot Teardown</strong></td>
                    <td <td style="text-align:center">{Hipot_Teardown}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Hipot DCW</strong></td>
                    <td <td style="text-align:center">{Hipot_DCW}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Hipot ACW</strong></td>
                    <td <td style="text-align:center">{Hipot_ACW}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Hold Process</strong></td>
                    <td <td style="text-align:center">{Hold_Process}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Hold Quality</strong></td>
                    <td <td style="text-align:center">{Hold_Quality}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Hold NPI</strong></td>
                    <td <td style="text-align:center">{Hold_NPI}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Hold Potting24h</strong></td>
                    <td <td style="text-align:center">{Hold_Potting24h}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Hold Potting20m</strong></td>
                    <td <td style="text-align:center">{Hold_Potting20m}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Hold Inventory</strong></td>
                    <td <td style="text-align:center">{Hold_Inventory}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Cell Cap</strong></td>
                    <td <td style="text-align:center">{Cell_Cap}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Cell Plug</strong></td>
                    <td <td style="text-align:center">{Cell_Plug}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Bond Redo</strong></td>
                    <td <td style="text-align:center">{Bond_Redo}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Adhesive Remove</strong></td>
                    <td <td style="text-align:center">{Adhesive_Remove}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Barb Damage</strong></td>
                    <td <td style="text-align:center">{Barb_Damage}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Tombstone Remove</strong></td>
                    <td <td style="text-align:center">{Tombstone_Remove}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Falcon Cap</strong></td>
                    <td <td style="text-align:center">{Falcon_Cap}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Falcon Prosthetic</strong></td>
                    <td <td style="text-align:center">{Falcon_Prosthetic}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>YDatum Prosthetic</strong></td>
                    <td <td style="text-align:center">{YDatum_Prosthetic}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Clamshell Patch</strong></td>
                    <td <td style="text-align:center">{Clamshell_Patch}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Current Collector Patch</strong></td>
                    <td <td style="text-align:center">{Current_Collector_Patch}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Sidemount Rework Other</strong></td>
                    <td <td style="text-align:center">{Sidemount_Rework_Other}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>FOD Remove</strong></td>
                    <td <td style="text-align:center">{FOD_Remove}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>IC Clamshell Gap</strong></td>
                    <td <td style="text-align:center">{IC_Clamshell_Gap}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Sidemount Foot</strong></td>
                    <td <td style="text-align:center">{Sidemount_Foot}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Cap Gap</strong></td>
                    <td <td style="text-align:center">{Cap_Gap}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Coupler Gap</strong></td>
                    <td <td style="text-align:center">{Coupler_Gap}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Sidemount Rework Other</strong></td>
                    <td <td style="text-align:center">{Sidemount_Rework_Other}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>End Cell</strong></td>
                    <td <td style="text-align:center">{End_Cell}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Spur Defect Check-In</strong></td>
                    <td <td style="text-align:center">{Spur_Defect_Check_In}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Pack Return Leak Test</strong></td>
                    <td <td style="text-align:center">{Pack_Return_Leak_Test}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Scrap REVIEW</strong></td>
                    <td <td style="text-align:center">{Scrap_REVIEW}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Potted CC Patch</strong></td>
                    <td <td style="text-align:center">{Potted_CC_Patch}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>New RepairTrial1</strong></td>
                    <td <td style="text-align:center">{New_RepairTrial1}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Use As Is</strong></td>
                    <td <td style="text-align:center">{Use_As_Is}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Add Potting</strong></td>
                    <td <td style="text-align:center">{Add_Potting}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Remove Potting From Sidemounts</strong></td>
                    <td <td style="text-align:center">{Remove_Potting_From_Sidemounts}</td>
                </tr>
                <tr>
                    <td <td style="text-align:left"><strong>Potted ReBond</strong></td>
                    <td <td style="text-align:center">{Potted_ReBond}</td>
                </tr>
    </tr>
    </table>"""
 
    webhook = 'teams_webhook_NCM_module_Output' if env == 'prod' else 'teams_webhook_DEV_Updates'
    creds = helper_functions.get_pw_json(webhook)
    webhookURL = creds['url'] 
    msg_title = 'NCM - Module Update'
    msg_summary = "Shift Update"
    sendTeamsMessage(webhookURL,msg_title,msg_summary,message)  
