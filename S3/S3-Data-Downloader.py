import boto3
from trp import Document
import pandas as pd
import os

bucket='3-stockchartx-database-design'
prefix = "1-modulus-data-analysis-project/Modulus Financial Engineering/StockChartX/Examples/C# 2010/"
destination = "New"

ACCESS_KEY='AKIAV6D5TVIDJV7UMVSK'
SECRET_KEY='nSKB3JeIWzPIglR4x4fpXqhth3ZX8FvDkcfZ4pQa'

client = boto3.client('s3', 
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
s3 = boto3.resource('s3',  
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)


def download_dir(prefix, local, bucket, client):
    """
    params:
    - prefix: pattern to match in s3
    - local: local path to folder in which to place files
    - bucket: s3 bucket with target contents
    - client: initialized s3 client object
    """
    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket':bucket,
        'Prefix':prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        for i in contents:
            k = i.get('Key')
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    for k in keys:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
        client.download_file(bucket, k, dest_pathname)




download_dir(prefix, destination, bucket, client)
