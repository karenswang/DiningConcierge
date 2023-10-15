import boto3
import json
from decimal import Decimal
from datetime import datetime

def lambda_handler(event, context):
    
    bucket_name = 'cloud-computing-yelp-data'
    file_name = 'restaurants_dynamodb.json'

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    restaurants_data = json.loads(obj['Body'].read().decode('utf-8'), parse_float=Decimal)
    
    insert_data(restaurants_data)

    return {
        'statusCode': 200,
        'body': json.dumps(f'Inserted {len(restaurants_data)} restaurants into DynamoDB')
    }
    
def insert_data(data_list, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    # overwrite if the same index is provided
    for data in data_list:
        # insertion timestamp
        data['insertedAtTimestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')  # ISO 8601 format
        response = table.put_item(Item=data)
    print('@insert_data: response', response)
    return response
