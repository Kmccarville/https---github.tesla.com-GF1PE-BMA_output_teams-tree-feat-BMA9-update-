import requests
from test import debug as masterdebug
import os
import logging
from xml.etree import ElementTree
from io import BytesIO
import pandas as pd
from io import StringIO
from github import Github
import helper_creds
import urllib.parse

debug=masterdebug
logging.basicConfig(level=logging.INFO)


if debug == True:
    with open('gf1pe_prod_cred.json') as f:
        lines = f.readlines()
        f.close()
        env='local_laptop'
        user='sa-prodengdb'
elif debug ==False:
    with open(r'/app/secrets/credentials') as f:
        lines = f.readlines()
        f.close()
        envk8s=os.getenv('ENVVAR2')
        env=os.getenv('ENVVAR3')
        user=os.getenv('ENVVAR4')

#get service account token to read github repos
sa_ghe_json=helper_creds.get_sa_ghe()
user=sa_ghe_json["user"]
password=urllib.parse.quote_plus(sa_ghe_json["password"])
g = Github(base_url="https://github.tesla.com/api/v3", login_or_token=password)

if env == "prod":
    branchstring = "master"
else:
    branchstring = "dev"

def bma123_output():
    repo = g.get_repo("GF1PE/GF1-PE-MySQL_Scripts",ref=branchstring)
    contents = repo.get_contents("bmaoutput.sql").decoded_content.decode()
    return contents

def bma4cta_output():
    repo = g.get_repo("GF1PE/GF1-PE-MySQL_Scripts",ref=branchstring)
    contents = repo.get_contents("bma4cta_output.sql").decoded_content.decode()
    return contents

def bma5cta_output():
    repo = g.get_repo("GF1PE/GF1-PE-MySQL_Scripts",ref=branchstring)
    contents = repo.get_contents("bma5cta_output.sql").decoded_content.decode()
    return contents

def bma4mamc_output():
    repo = g.get_repo("GF1PE/GF1-PE-MySQL_Scripts",ref=branchstring)
    contents = repo.get_contents("bma4mamc_output.sql").decoded_content.decode()
    return contents

def bma5mamc_output():
    repo = g.get_repo("GF1PE/GF1-PE-MySQL_Scripts",ref=branchstring)
    contents = repo.get_contents("bma5mamc_output.sql").decoded_content.decode()
    return contents

def bma4c3a_output():
    repo = g.get_repo("GF1PE/GF1-PE-MySQL_Scripts",ref=branchstring)
    contents = repo.get_contents("bma4c3a_output.sql").decoded_content.decode()
    return contents

def bma5c3a_output():
    repo = g.get_repo("GF1PE/GF1-PE-MySQL_Scripts",ref=branchstring)
    contents = repo.get_contents("bma5c3a_output.sql").decoded_content.decode()
    return contents

def zone4_output():
    repo = g.get_repo("GF1PE/GF1-PE-MySQL_Scripts",ref=branchstring)
    contents = repo.get_contents("bmaZ4_output.sql").decoded_content.decode()
    return contents