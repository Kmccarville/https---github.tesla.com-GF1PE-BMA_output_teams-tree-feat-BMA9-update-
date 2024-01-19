import json
import logging
import os
import sys
import traceback

import requests

debug=False
mattermostBotName = "Error Bot"
mattermostIconUrl = "https://image.flaticon.com/icons/svg/196/196759.svg"

def e_handler(e, trace=None, subject=None, handleType=0):
    '''
    Error handle to handle any internal errors or errors passed to the handle_error endpoint
    e: The exception that occured
    trace: (if called from endpoint) the stack trace
    subject: (if called from endpoint) the message subject
    handleType: How the error is to be handled. 0=email, 1=mattermost, 2=both
    '''
    var = trace if trace is not None else traceback.format_exc()
    html=f'''<html>
    <body>
    <h1>Exception Summary</h1>
    <p>{e}</p>
    <h1>Traceback Error</h1>
    <p>{var}</p>
    </body>
    </html>
    '''
    payload={"text": "Error Exception", "html":html, "recipients":"mberlied@tesla.com", "cc":"mberlied@tesla.com", "Bcc":"mberlied@tesla.com"}
    subject = subject if subject is not None else "BMA Teams Poster Error Exception"

    if debug:
        payload["subject"] = f"Local: {subject}"
        logging.info("ERROR, would send email : " + str(e))
    else:
        branch = os.getenv('ENVVAR3')
        if branch == "prod":
            subject = f"CRITICAL: {subject} from {branch}"
        else:
            subject = f"NON CRITICAL: {subject} from {branch}"
        payload["subject"] = subject

        if handleType == 0 or handleType == 2: #call email script here
            email = requests.post('https://prodengdbapi.mo.tesla.services/api/email', data=json.dumps(payload))

    if handleType not in [0, 1, 2]:
        logging.info("Not a valid handle type")
