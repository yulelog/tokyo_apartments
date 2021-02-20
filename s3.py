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
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)

    CLIENT.put_object(
        Body = csv_buffer.getvalue(),
        Bucket = BUCKET,
        Key = filename
    )


def save_image(image_url, property_id):
    """
    Given a url to a property's floorplan image, save the image down locally with property id
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
