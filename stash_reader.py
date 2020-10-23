import requests
from test import debug as masterdebug
import os
import logging
from xml.etree import ElementTree
from io import BytesIO
import pandas as pd
from io import StringIO

debug=masterdebug

logging.basicConfig(level=logging.INFO)


if debug == True:
    with open('pass.txt') as f:
        lines = f.readlines()
        f.close()
        env='local_laptop'
        user='sa-prodengdb'
elif debug ==False:
    with open(r'/app/secrets/credentials') as f:
        lines = f.readlines()
        f.close()
        env=os.getenv('ENVVAR2')
        user=os.getenv('ENVVAR4')
password=lines[5]
password=str(password)
password=str.strip(password)




def bmaoutput():
    url = 'https://stash.teslamotors.com/projects/GF1PE/repos/gf1-pe-mysql-scripts/raw/bmaoutput.sql'
    r = requests.get(url, auth=(user, password ))
    return r.text

def bma4cta_output():
    url = 'https://stash.teslamotors.com/projects/GF1PE/repos/gf1-pe-mysql-scripts/raw/bma4cta_output.sql'
    r = requests.get(url, auth=(user, password ))
    return r.text

def bma5cta_output():
    url = 'https://stash.teslamotors.com/projects/GF1PE/repos/gf1-pe-mysql-scripts/raw/bma5cta_output.sql'
    r = requests.get(url, auth=(user, password ))
    return r.text

def bma4mamc_output():
    url = 'https://stash.teslamotors.com/projects/GF1PE/repos/gf1-pe-mysql-scripts/raw/bma4mamc_output.sql'
    r = requests.get(url, auth=(user, password ))
    return r.text

def bma5mamc_output():
    url = 'https://stash.teslamotors.com/projects/GF1PE/repos/gf1-pe-mysql-scripts/raw/bma5mamc_output.sql'
    r = requests.get(url, auth=(user, password ))
    return r.text

def bma4c3a_output():
    url = 'https://stash.teslamotors.com/projects/GF1PE/repos/gf1-pe-mysql-scripts/raw/bma4c3a_output.sql'
    r = requests.get(url, auth=(user, password ))
    return r.text

def bma5c3a_output():
    url = 'https://stash.teslamotors.com/projects/GF1PE/repos/gf1-pe-mysql-scripts/raw/bma5c3a_output.sql'
    r = requests.get(url, auth=(user, password ))
    return r.text