import pandas as pd
import os
import requests
import pprint
from dateutil import parser
from dateutil import relativedelta
import time, datetime
# Import smtplib for the actual sending function
import smtplib
# Import the email modules we'll need
from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from test import debug as masterdebug
import base64
from email import encoders

debug=False


#A function to 3actually send the email
def SendEmail(text, html, rec, ccEmail, BccEmail, Subject,**attachment):
  # Create message container - the correct MIME type is multipart/alternative.
  msg = MIMEMultipart('alternative')
  
  msg['Subject'] = Subject
  if debug==True:
    msg['From'] = 'rpivisiong@tesla.com'  
  elif debug==False:
    msg['From'] =os.getenv('ENVVAR1') 
  msg['To'] = rec
  msg["Bcc"] = BccEmail
  msg["cc"]=ccEmail
  
  # Record the MIME types of both parts - text/plain and text/html.
  part1 = MIMEText(text, 'plain')
  part2 = MIMEText(html, 'html')
  
  # Attach parts into message container.
  # According to RFC 2046, the last part of a multipart message, in this case
  # the HTML message, is best and preferred.
  msg.attach(part1)
  msg.attach(part2)

  if ('attachment1' in attachment):
    if ('attachment1_name' in attachment):
      part = MIMEBase('application', 'octet-stream')
      part.set_payload(attachment['attachment1'].getvalue())
      encoders.encode_base64(part)
      part.add_header('Content-Disposition', 'attachment; filename= "%s"' % attachment['attachment1_name'])
      msg.attach(part)

  
  try:
    # Send the message via the Tesla internal SMTP server.
    s = smtplib.SMTP('smtp-int.teslamotors.com')
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    print("In the SendEmail function, now sending email")
    s.sendmail(msg['From'], msg["To"].split(";")+msg["cc"].split(";"), msg.as_string())
  except smtplib.SMTPException as e:
    print(str(e)) 
  
  s.quit()

def SendEmail_manual_input(email_sender,text, html, rec, ccEmail, BccEmail, Subject,**attachment):
  # Create message container - the correct MIME type is multipart/alternative.
  msg = MIMEMultipart('alternative')
  
  msg['Subject'] = Subject
  if debug==True:
    msg['From'] = email_sender  
  elif debug==False:
    msg['From'] =email_sender 
  msg['To'] = rec
  msg["Bcc"] = BccEmail
  msg["cc"]=ccEmail
  
  # Record the MIME types of both parts - text/plain and text/html.
  part1 = MIMEText(text, 'plain')
  part2 = MIMEText(html, 'html')
  
  # Attach parts into message container.
  # According to RFC 2046, the last part of a multipart message, in this case
  # the HTML message, is best and preferred.
  msg.attach(part1)
  msg.attach(part2)
  for i in range(1,10):
    if (f'attachment{i}' in attachment):
      if (f'attachment{i}_name' in attachment):
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment[f'attachment{i}'].getvalue())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename= "%s"' % attachment[f'attachment{i}_name'])
        msg.attach(part)

  
  try:
    # Send the message via the Tesla internal SMTP server.
    s = smtplib.SMTP('smtp-int.teslamotors.com')
    #s.starttls()
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    print("In the SendEmail function, now sending email")
    s.sendmail(msg['From'], msg["To"].split(";")+msg["cc"].split(";"), msg.as_string())
  except smtplib.SMTPException as e:
    print(str(e)) 
  
  s.quit()

