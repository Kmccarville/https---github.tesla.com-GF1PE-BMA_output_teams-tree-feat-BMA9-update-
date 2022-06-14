import json
import os
# from test import debug as masterdebug

debug=False

if debug==True :
    is_deployed = False  # Means running on Kube  NOT on local machine
    is_prod = False  # Use the production tables OR Dev/Test Tables
    is_kube = False  # Deployed on Kubernetes or Other
else:
    is_deployed = True  # Means running on Kube  NOT on local machine
    is_prod = False  # Use the production tables OR Dev/Test Tables
    is_kube = True  # Deployed on Kubernetes or Other

if not is_deployed:
    with open('gf1pe_prod_cred.json') as f:
        pw_json = json.load(f)
        f.close()
elif is_kube:
    with open(r'/app/secrets/credentials') as f:
        pw_json = json.load(f)
        f.close()


# Add more of these functions as needed
def get_demo():
    return pw_json["demo"]

def get_trv():
    return pw_json["task_result_viewer"]

def get_mos_base_info():
    return pw_json["mos_base_info"]

def get_prodengdb():
    return pw_json["prodeng_db"]

def get_mos_rpt2():
    return pw_json["mos_rpt2"]

def get_plc_db():
    return pw_json['plc_db']

def get_gf1_pallet_mgt():
    return pw_json["gf1_pallet_management"]

def get_clickhouse():
    return pw_json["clickhouse"]

def get_mos_rpt2_db():
    return pw_json["mos_rpt2_db"]

def get_pc_db():
    return pw_json["pc_db"]

def get_jira():
    return pw_json["jira_creds"]

def get_sa_ghe():
    return pw_json["sa_ghe"]

def get_sa_jira():
    return pw_json["sa_jira"]

def get_ignition_db():
    return pw_json["ignition_dev_db_cta"]

def get_pallet_record_db():
    return pw_json["pallet_record"]

def get_mos_db():
    return pw_json["mosrpt1"]

def get_flux_token():
    return pw_json["kubeless_fluxcounter_token"]

def get_teams_webhook_BMA123():
    return pw_json["teams_webhook_BMA123_Updates"]

def get_teams_webhook_BMA45():
    return pw_json["teams_webhook_BMA45_Updates"]

def get_teams_webhook_Z3():
    return pw_json["teams_webhook_Zone3_Updates"]

def get_teams_webhook_Z4():
    return pw_json["teams_webhook_Zone4_Updates"]

def get_teams_webhook_MY3():
    return pw_json["teams_webhook_MY3_Leadership"]

def get_teams_webhook_DEV():
    return pw_json["teams_webhook_DEV_Updates"]