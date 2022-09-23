import json
import requests
from requests.exceptions import Timeout
import sqlalchemy
from urllib.parse import quote

def file_reader(FilePath):
    with open(FilePath,"r") as f:
        contents = f.read()
        return contents

with open(r'/app/secrets/credentials') as f:
    pw_json = json.load(f)
    f.close()

def get_pw_json(key):
    return pw_json[key]

def get_sql_conn(key, schema=None):
    cred = get_pw_json(key)
    # Pull database credentials
    user = cred['user']
    password = quote(cred['password'])
    hostname = cred['host']
    port = cred['port']

    schema_str = f"/{schema}" if schema else ""
    # Define database connection
    engine = sqlalchemy.create_engine(f'mysql+pymysql://{user}:{password}@{hostname}:{port}{schema_str}')
    # Return connection to engine
    return engine.connect()

def send_to_teams(webhook_key, title, html,retry=0):
    webhook_json = get_pw_json(webhook_key)
    webhook = webhook_json['url']
    payload=    {
                "title":title, 
                "summary":"summary",
                "sections":[{'text':html}]
                }
    headers = {
    'Content-Type': 'application/json'
    }
    try:
        requests.post(webhook,timeout=10,headers=headers, data=json.dumps(payload))
    except Timeout:
        if retry==1:
            requests.post(webhook,timeout=10,headers=headers, data=json.dumps(payload))