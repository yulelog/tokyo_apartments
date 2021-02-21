import boto3
import pandas as pd
import requests
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
    """
    Load dataframe in csv to s3, folder name is specified in filename
    :param df: pandas dataframe
    :param filename: filename including the folder name in the s3 bucket
    :return : nothing
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    CLIENT.put_object(
        Body = csv_buffer.getvalue(),
        Bucket = BUCKET,
        Key = filename
    )


def save_image(image_url, property_id):
    """
    Given a url to a property's floorplan image, save the image to s3 bucket/floor_plans folder with property id
    :param image_url: string of the url to the image
    :param property_id: the string of property id
    :return : nothing
    """
    # Open the url image, set stream to True, this will return the stream content.
    r = requests.get(image_url, stream = True)

    # Check if the image was retrieved successfully
    if r.status_code == 200:
        # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
        # r.raw.decode_content = True
        req_data = r.raw.read()
        filename = f"floor_plans/{property_id}.jpg"
        CLIENT.put_object(
            Body = req_data,
            Bucket = BUCKET,
            Key = filename
        )
    else:
        LOGGER.error(f"[ERROR] Unable to download the floorplan image of property: {property_id}")

def read_from_s3(filename):
    """
    Read dataframe from csv file in s3
    :param filename: filename including the folder name in the s3 bucket
    :return : pandas dataframe
    """
    csv_obj = CLIENT.get_object(Bucket=BUCKET, Key=filename)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_string))
    return df