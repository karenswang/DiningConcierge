from datetime import datetime, timezone, timedelta
import boto3

# Validate location
def validate_location(location):
    return location.lower() in ['manhattan', 'new york']
    
# Validate cuisine
def validate_cuisine(input_text):
    # Currently only support six 
    cuisines = ['korean', 'chinese', 'italian', 'mexican', 'thai', 'japanese']
    for cuisine in cuisines:
        if cuisine in input_text.lower():
            return cuisine
    return None

# Validate date and make sure it's in the future
def validate_date(date_text):
    # Disclosure: I referenced information from ChatGPT to debug this time zone conversion issue
    # Consider the time zone difference to avoid issues with "today" entry
    
    # Need to get a naive datatime object, otherwise there's a TypeError: can't compare offset-naive and offset-aware datetimes
    input_date_naive = datetime.strptime(date_text, '%Y-%m-%d')

    eastern = timezone(timedelta(hours=-4))  # EDT
    # Convert the naive datetime object to be timezone-aware
    input_date = input_date_naive.replace(tzinfo=eastern)

    # Get the current date in Eastern Time
    current_date_eastern = datetime.now(eastern).date()
    
    return input_date.date() >= current_date_eastern

# Validate datetime and make sure it's in the future
def validate_datetime(date_text, time_text):
    datetime_str = f"{date_text} {time_text}"
    
    # Parse input datetime as a "naive" datetime object
    input_datetime_naive = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
    
    eastern = timezone(timedelta(hours=-4)) #EDT
    
    # Convert the naive datetime object to be timezone-aware
    input_datetime = input_datetime_naive.replace(tzinfo=eastern)
    
    # Get the current datetime in Eastern Time
    current_datetime_eastern = datetime.now(eastern)
    
    return input_datetime > current_datetime_eastern

def validate_party_size(size):
    try:
        return int(size) > 0
    except ValueError:
        return False

# Send message to SQS queue
def send_to_sqs(slots):
    # Initialize
    sqs = boto3.client('sqs', region_name='us-east-1')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/488249905444/Q1'

    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=(str(slots))
        )
        print(f"Message Sent to SQS: {response['MessageId']}")
        print("Message is: ", str(slots))
    except Exception as e:
        print(f"Error sending message to SQS: {str(e)}")
    

def lambda_handler(event, context):
    print(event["inputTranscript"])
    print(event)
    
    slots = event['sessionState']['intent']['slots']
    resp = {"statusCode": 200, "sessionState": event["sessionState"]}

    # Check if all slots are None (initial triggering stage)
    if all(slot is None for slot in slots.values()):
        if "proposedNextState" not in event:
            resp["sessionState"]["dialogAction"] = {"type": "Close"}
        else:
            resp["sessionState"]["dialogAction"] = event["proposedNextState"]["dialogAction"]
        return resp
    
    # Validate Location
    if slots['Location'] is not None:
        if not validate_location(slots['Location']['value']['interpretedValue']):
            resp["sessionState"]["dialogAction"] = {'type': 'ElicitSlot', 'slotToElicit': 'Location'}
            return resp
    
    # Validate Cuisine
    if slots['Cuisine'] is not None:
        validated_cuisine = validate_cuisine(slots['Cuisine']['value']['originalValue'])
        if validated_cuisine:
            slots['Cuisine']['value']['interpretedValue'] = validated_cuisine
        else:
            resp["sessionState"]["dialogAction"] = {'type': 'ElicitSlot', 'slotToElicit': 'Cuisine'}
            return resp
    
    # Validate Dining Date
    if slots['DiningDate'] is not None:
        if not validate_date(slots['DiningDate']['value']['interpretedValue']):
            resp["sessionState"]["dialogAction"] = {'type': 'ElicitSlot', 'slotToElicit': 'DiningDate'}
            return resp

    # Validate Dining Time
    if slots['DiningTime'] is not None and slots['DiningDate'] is not None:
        if not validate_datetime(slots['DiningDate']['value']['interpretedValue'], slots['DiningTime']['value']['interpretedValue']):
            resp["sessionState"]["dialogAction"] = {'type': 'ElicitSlot', 'slotToElicit': 'DiningTime'}
            return resp

    # Validate Party Size
    if slots['PartySize'] is not None:
        if not validate_party_size(slots['PartySize']['value']['interpretedValue']):
            resp["sessionState"]["dialogAction"] = {'type': 'ElicitSlot', 'slotToElicit': 'PartySize'}
            return resp

    # Proceed to the proposed next state if it exists and all slots validated
    if "proposedNextState" in event:
        resp["sessionState"]["dialogAction"] = event["proposedNextState"]["dialogAction"]
    else:
        resp["sessionState"]["dialogAction"] = {"type": "Close"}
        send_to_sqs(slots)

    return resp
