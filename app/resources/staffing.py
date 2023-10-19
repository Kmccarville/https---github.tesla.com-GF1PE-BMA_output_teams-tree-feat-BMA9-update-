import pandas as pd
import sqlalchemy
from urllib.parse import quote
from datetime import datetime,timedelta
import pytz
import helper_functions

def main(env):
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

  assembly_lines = ['Grand Total',
                    'Module-BMA123-Zone1','Module-BMA123-Zone2',
                    'Module-BMA45-Zone1','Module-BMA45-Zone-2',
                      'Module-BMA8-Zone1','Module-BMA8-Zone2',
                      'Module-Zone3-123','Module-Zone3-45',
                      'Module-Zone4-MC1','Module-Zone4-MC2',
                      'Module-Zone-1-Bando-Rework',
                      'Kitting&Fracking']

  hc_goals = [20,36,32,33,8,12,21,21,19,24,39,14]

  hc_goals.insert(0,sum(hc_goals))

  df_all = pd.DataFrame({
                          'Assembly Line' : assembly_lines,
                          'Goal' : hc_goals
                      })

  categories = ['Present','Absent','Call Out','Time Off','Unscheduled']
  for category in categories:
      df_sub = df.query(f"`{category}`==1")
      df_summary = df_sub.groupby("Assembly Line")[category].count()
      df_summary.loc['Grand Total'] = len(df_sub)
      df_summary = df_summary.reset_index()
      df_all = df_all.merge(df_summary,how='outer',on='Assembly Line')

  df_all.fillna(0,inplace=True)
  df_all['Goal'] = df_all['Goal'].astype(int)
  for category in categories:
      df_all[category] = df_all[category].astype(int)

  webhook_key = 'teams_webhook_staffing' if env=='prod' else 'teams_webhook_DEV_Updates'
  send_alert(webhook_key,'',df_all,caption="Battery Module Staffing Alert",link_title="LiveStaffing",link_button="https://bi.teslamotors.com/#/views/BatteryModuleLaborAnalytics/LiveStaffing?:iid=1")
  # redden = lambda x: ['color:red']*len(x) if x.Present < x.Goal else ['']*len(x)
  # msg = df_all.style.apply(redden, axis=1).render()
