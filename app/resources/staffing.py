import logging
from datetime import datetime, timedelta
from urllib.parse import quote

import common.helper_functions as helper_functions
import pandas as pd
import pymsteams
import pytz
import sqlalchemy
from common.constants import STAFFING_GREEN, TESLA_RED


def main(env):
  logging.info("Staffing Main Started")
  pst = pytz.timezone('US/Pacific')
  utc = pytz.timezone('UTC')
  rn = datetime.utcnow()
  utc_now=utc.localize(rn)
  pst_now = utc_now.astimezone(pst)
  pst_now_str = str(pst_now).split(' ')[0] + ' 00:00:00'

  the_shift = '1st Shift' if pst_now.hour < 18 and pst_now.hour >= 6 else '2nd Shift'

  edw_con = helper_functions.get_sql_conn('edwro',schema=None,conn_type='mssql')
  query = f"""
          SELECT *
          FROM Kronos_DM.dbo.TESLA_PLANNED_VS_PRESENT t
          WHERE [Cost Center Code] = '30630100'
          and [Job Desc] like '%production%associate%'
          """
  df = pd.read_sql(query,edw_con)
  edw_con.close()

  df = df.query(f"`MH Shift Group` == '{the_shift}' and `Event Date`=='{pst_now_str}'")
  
  #get staffing targets
  prodeng_conn = helper_functions.get_sql_conn('prodeng_ro')
  query = f"SELECT `Assembly Line`, `Goal` from gf1pe_bm_global._static_staffing_targets WHERE Shift='{the_shift}'"
  df_all = pd.read_sql(query,prodeng_conn)
  prodeng_conn.close()
  df_gt = pd.DataFrame({'Assembly Line' : ['Grand Total'], 'Goal' : [df_all['Goal'].sum()]})
  df_all = pd.concat([df_gt,df_all],axis=0)

  categories = ['Unscheduled','Present','Absent','Call Out','Time Off']
  for category in categories:
      df_sub = df.query(f"`{category}`==1")
      df_summary = df_sub.groupby("Assembly Line")[category].count()
      df_summary.loc['Grand Total'] = len(df_sub)
      df_summary = df_summary.reset_index()
      df_all = df_all.merge(df_summary,how='left',on='Assembly Line')

  df_all.fillna(0,inplace=True)
  df_all['Goal'] = df_all['Goal'].astype(int)
  for category in categories:
      df_all[category] = df_all[category].astype(int)
  
  df_all.loc[:,'TOTAL'] = df_all['Present'] + df_all['Absent'] + df_all['Call Out'] + df_all['Unscheduled'] + df_all['Time Off']
  df_all.loc[:,'AbsentPercent'] = ((df_all['Absent'] + df_all['Call Out'])/df_all['TOTAL'])*100
  df_all.loc[:,'Attainment'] = (df_all['Present'] + df_all['Unscheduled'])/df_all['Goal']*100
  df_total = df_all.query("`Assembly Line` == 'Grand Total'")
  total_attainment = (df_total['Present'].sum() + df_total['Unscheduled'].sum())/df_total['Goal'].sum()*100
  #forming the html
  headers = categories
  headers.insert(0,'Goal')
  headers.insert(0,'Attainment')
  headers.insert(0,'Assembly Line')
  headers.insert(len(headers),'Absent&CallOut %')
  header_html = "<tr>"
  for header in headers:
      align_type = 'left' if header=='Assembly Line' else 'center'
      header_html += f"""<th style="text-align:{align_type}">{header}</th>"""

  header_html+="</tr>"
  attainment_threshold = 95
  sub_html = ""
  for index,row in df_all.iterrows():
      color_text = "color:red" if row.Attainment < attainment_threshold else "color:green"
      sub_html +=  f"""
              <tr>
                  <td style="text-align:left">{row['Assembly Line']}</td>
                  <td style="text-align:center;{color_text}">{row.Attainment:.0f}%</td>
                  <td style="text-align:center">{row.Goal}</td>
                  <td style="text-align:center">{row.Unscheduled}</td>
                  <td style="text-align:center">{row.Present}</td>
                  <td style="text-align:center">{row.Absent}</td>
                  <td style="text-align:center">{row['Call Out']}</td>
                  <td style="text-align:center">{row['Time Off']}</td>
                  <td style="text-align:center">{row['AbsentPercent']:.0f}%</td>
              </tr>
              """

  full_html = "<table>" + header_html + sub_html + "</table>"
  
  #webhook
  webhook_key = 'teams_webhook_staffing' if env=='prod' else 'teams_webhook_DEV_Updates'
  webhook_json = helper_functions.get_pw_json(webhook_key)
  webhook = webhook_json['url']

  #making the teams message
  teams_msg = pymsteams.connectorcard(webhook)
  title = f"Battery Module SOS Staffing Report ({total_attainment:.0f}%)"
  teams_msg.title(title)
  teams_msg.summary('SOS-Staffing')
  msg_color = TESLA_RED if total_attainment < attainment_threshold else STAFFING_GREEN
  teams_msg.color(msg_color)
  staff_card = pymsteams.cardsection()
  staff_card.text(full_html)
  teams_msg.addSection(staff_card)
  teams_msg.addLinkButton("LiveStaffingDash", "https://bi.teslamotors.com/#/views/BatteryModuleLaborAnalytics/LiveStaffing?:iid=1")
  
  #SEND IT
  try:
      teams_msg.send()
  except pymsteams.TeamsWebhookException:
      logging.warn("Webhook timed out, retry once")
      try:
          teams_msg.send()
      except pymsteams.TeamsWebhookException:
          logging.exception("Webhook timed out twice -- pass to next area")

  logging.info("Staffing Main Finished")
