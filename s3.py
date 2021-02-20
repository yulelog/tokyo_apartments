import boto3
import pandas as pd
from io import StringIO

ACCESS_KEY = pd.read_csv('rootkey.csv').to_dict('records')[0]
CLIENT = boto3.client(
    's3',
    aws_access_key_id = ACCESS_KEY['AWSAccessKeyId'],
    aws_secret_access_key = ACCESS_KEY['AWSSecretKey'],
    region_name = 'ap-northeast-1'
    )
BUCKET = 'tokyo-apartments'

def load_to_s3(df, filename):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)

    response = CLIENT.put_object(
        Body = csv_buffer.getvalue(),
        Bucket = BUCKET,
        Key = filename
    )
