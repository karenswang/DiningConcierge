import json
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    sqs = boto3.client('sqs')
    # Get message from SQS
    response = sqs.receive_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/488249905444/Q1',
        MaxNumberOfMessages=1,
    )
    print("checking response ... ", response)
    if 'Messages' in response:
    # Extract messages
        messages = response['Messages']
        
        for message in messages:
            print("MessageId:", message['MessageId'])
            print("ReceiptHandle:", message['ReceiptHandle'])
            print("MD5OfBody:", message['MD5OfBody'])
            
            # Extracting Body and converting string to dictionary
            body = eval(message['Body'])
    
            location = body['Location']['value']['interpretedValue']
            cuisine = body['Cuisine']['value']['interpretedValue']
            date = body['DiningDate']['value']['interpretedValue']
            time = body["DiningTime"]['value']['interpretedValue']
            size = body['PartySize']['value']['interpretedValue']
            email = body['Email']['value']['interpretedValue']
            
            # get restaurant recommendation
            recommendation = get_recommendation(cuisine)
            # print(recommendation)
            
            rec_body = json.loads(recommendation['body'])
            rec_results = rec_body['results']
            
            # Extract restaurant id
            restaurant_ids = [restaurant['RestaurantID'] for restaurant in rec_results]
            business_names = []
            business_addresses = []

            # For each restaurant id, find its name and address from dynamodb
            for restaurant_id in restaurant_ids:
                final_result = lookup_data_dynamodb({'business_id': restaurant_id})
                business_name = final_result['name']
                business_address = final_result['address']
                business_names.append(business_name)
                business_addresses.append(business_address)

            text_message = f"Hello! Here are my {cuisine} restaurant suggestions for {size} people, for {date} at {time}: 1. {business_names[0]}, located at {business_addresses[0]}; 2. {business_names[1]}, located at {business_addresses[1]}; 3. {business_names[2]}, located at {business_addresses[2]}."
            print(text_message)
            print(f"Sending email to: {email}")
            # Send email with formatted message to the user email
            send_email(email, "Your restaurant recommendation", text_message)
            
            # Delete processed message from SQS
            handle = message['ReceiptHandle']
            sqs.delete_message(
                QueueUrl='https://sqs.us-east-1.amazonaws.com/488249905444/Q1',
                ReceiptHandle=handle
            )
            
    else:
        print("No messages available at this time.")
    
    return response

    
def send_email(to_email, subject, body_text):
    sender = "sw3709@columbia.edu"

    client = boto3.client('ses', region_name='us-east-1')

    # Try to send the email
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    to_email,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': "UTF-8",
                        'Data': body_text,
                    },
                },
                'Subject': {
                    'Charset': "UTF-8",
                    'Data': subject,
                },
            },
            Source=sender,
        )

    # Catch error
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


def lookup_data_dynamodb(key, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.get_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response['Item'])
        return response['Item']


REGION = 'us-east-1'
HOST = 'search-restaurants-dnlxvpmopiqturwtzvzdwbvf7q.us-east-1.es.amazonaws.com'
INDEX = 'restaurants'

def query(term):
    q = {'size': 3, 'query': {'multi_match': {'query': term}}}

    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)

    res = client.search(index=INDEX, body=q)
    print(res)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])

    return results


def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)


def get_recommendation(cuisine):
    results = query(cuisine)
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
        },
        'body': json.dumps({'results': results})
    }
