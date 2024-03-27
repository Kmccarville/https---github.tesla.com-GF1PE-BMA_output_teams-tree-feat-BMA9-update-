
from . import helper_functions as hf
import logging
import boto3
from botocore.exceptions import ClientError
import io
import matplotlib.pyplot as plt
from datetime import datetime
import tempfile
import pandas as pd
import json

def upload_obj(s3_client, buffer, bucket, key):
    try:
        metadata = {'ContentDisposition': 'inline'}
        s3_client.upload_fileobj(buffer, bucket, key, ExtraArgs={'Metadata': metadata})

        print(f"File uploaded successfully to '{bucket}' with key '{key}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

def get_obj_url(s3_client, bucket, key):
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params = {
                'Bucket': bucket, 
                'Key': key,
                'ResponseContentDisposition': 'inline',
                'ResponseContentType': 'image/jpeg'
            },
            ExpiresIn=8000 #86400
        )
    
    except ClientError as e:
        print("Error generating presigned URL:", e)
        return None
    
    return url

def delete_all_objects(s3_client, bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    
    if 'Contents' in response:
        
        for obj in response['Contents']:
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        
        while response['KeyCount'] >= 1000:
            response = s3_client.list_objects_v2(Bucket=bucket_name, ContinuationToken=response['NextContinuationToken'])
            for obj in response['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
    
    print("All objects deleted from the bucket.")
    
def bucket_init():
    endpoint = hf.get_pw_json('xplot')['endpoint']
    access_key = hf.get_pw_json('xplot')['access_key']
    secret = hf.get_pw_json('xplot')['secret']
    certificate = json.dumps(hf.get_pw_json('xplot')['certificate'])
    certificate = certificate.replace('\\\\n', '\n')
    
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(certificate.encode('utf-8'))
    s3_client = boto3.client('s3', 
                             aws_access_key_id=access_key, 
                             aws_secret_access_key=secret,
                             endpoint_url=endpoint,
                             verify=temp_file.name)
    return s3_client
               
def get_line_name(line_id):
    pedb_con = hf.get_sql_conn('pedb', schema='gf1pe_bm_global')
    query = f"""
        SELECT LINE as NAME FROM gf1pe_bm_global._static_lines
        WHERE LINE IS NOT NULL AND ID={line_id};   
    """
    df = pd.read_sql(query, pedb_con)
    pedb_con.close()
    return df['NAME'].iloc[0]

def get_plot(plot_map):
    lines = []
    fig, ax = plt.subplots(2, 2)
    fig.subplots_adjust(wspace=0.6, hspace=0.6)
    fig.suptitle('A look at the past 12 hours.')
    
    for _, (line_id, outputs) in enumerate(plot_map.items()):
        _id = str(line_id)[0]
        if '4' == _id:
            axe = ax[1, 1]
            axe.set_title('Zone 4')
        elif '3' == _id:
            axe = ax[0, 1]
            axe.set_title('Zone 3')
        elif '2' == _id:
            axe = ax[1, 0]
            axe.set_title('Zone 2')
        elif '1' == _id:
            axe = ax[0, 0]
            axe.set_title('Zone 1')
        axe.set_xlabel("Hours")
        axe.set_ylabel("Carsets")
        label = get_line_name(line_id)
        line, = axe.plot(outputs, label=label)
        axe.legend(fontsize=4)
        lines.append(line)
    buffer = io.BytesIO()
    
    plt.savefig(buffer, format='png', dpi=250)
    buffer.seek(0)
    return buffer
    
    
def get_zone1():
    pedb_con = hf.get_sql_conn('pedb', schema='teams_output')
    query = """
        SELECT LINE_ID, TOTAL FROM teams_output.zone1
        WHERE END_TIME >= NOW() - INTERVAL 12 HOUR; 
    """
    df = pd.read_sql(query, pedb_con)
    pedb_con.close()
    return [tuple(x) for x in df.to_records(index=False)]

def get_zone2():
    pedb_con = hf.get_sql_conn('pedb', schema='teams_output')
    query = """
        SELECT master.LINE_ID, master.MAMC_OUTPUT + master.C3A_OUTPUT AS TOTAL FROM (
            SELECT LINE_ID, a.MAMC_OUTPUT, a.C3A_OUTPUT FROM teams_output.zone2_bma123 AS a
            WHERE a.END_TIME >= NOW() - INTERVAL 12 HOUR
            UNION ALL
            SELECT b.LINE_ID, b.MAMC_OUTPUT, b.C3A_OUTPUT FROM zone2_bma45 AS b
            WHERE b.END_TIME >= NOW() - INTERVAL 12 HOUR
            UNION ALL
            SELECT 28, c.MAMC_OUTPUT, c.C3A_OUTPUT FROM zone2_bma8 as c
            WHERE c.END_TIME >= NOW() - INTERVAL 12 HOUR
        ) AS master
    """
    df = pd.read_sql(query, pedb_con)
    pedb_con.close()
    return [tuple(x) for x in df.to_records(index=False)]

def get_zone3():
    pedb_con = hf.get_sql_conn('pedb', schema='teams_output')
    query = """
        SELECT LINE_ID, CARSETS AS TOTAL FROM teams_output.zone3
        WHERE END_TIME >= NOW() - INTERVAL 12 HOUR;  
    """
    df = pd.read_sql(query, pedb_con)
    pedb_con.close()
    return [tuple(x) for x in df.to_records(index=False)]

def get_zone4():
    pedb_con = hf.get_sql_conn('pedb', schema='teams_output')
    query = """
        SELECT LINE_ID, UPH AS TOTAL FROM teams_output.zone4
        WHERE END_TIME >= NOW() - INTERVAL 12 HOUR; 
    """
    df = pd.read_sql(query, pedb_con)
    pedb_con.close()
    return [tuple(x) for x in df.to_records(index=False)]

def get_plot_map():
    plot_map = {}
    zone1_tuple_list = get_zone1()
    zone2_tuple_list = get_zone2()
    zone3_tuple_list = get_zone3()
    zone4_tuple_list = get_zone4()
    for tuple_list in [zone1_tuple_list, zone2_tuple_list, zone3_tuple_list, zone4_tuple_list]:
        for _tuple in tuple_list:
            line_id = _tuple[0]
            val = _tuple[1]
            if line_id in plot_map:
                plot_map[line_id].append(val)
            else:
                plot_map[line_id] = [val]  
    return plot_map
    
def get_link():
    plot_map = get_plot_map()
    bucket = hf.get_pw_json('xplot')['bucket_name']
    d = datetime.today()
    year = d.year
    hour = d.hour
    day = d.day
    eos_date = f'{day}_{hour}_{year}'
    obj_key = f'eos{eos_date}.png'
    buffer = get_plot(plot_map)
    s3_client = bucket_init()
    upload_obj(s3_client, buffer, bucket, obj_key)
    obj_url = get_obj_url(s3_client, bucket, obj_key)
    buffer.close()
    return obj_url