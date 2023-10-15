import boto3
import json

client = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    try:
        # Extracting message from BotRequest
        messages = event['messages']
        
        response_messages = []
        for message in messages:
            unstructured = message['unstructured']
            msg_from_user = unstructured['text']  
            
            # Initiating Lex
            lex_response = client.recognize_text(
                botId='O9GTZWO5XZ',
                botAliasId='OPF4AQXSRH',
                localeId='en_US',
                sessionId='testuser',
                text=msg_from_user)
            
            # Extracting message from Lex response
            msg_from_lex = lex_response.get('messages', [])
            if msg_from_lex:
                response_text = msg_from_lex[0]['content']
            else:
                response_text = "Sorry, I didn't understand that."
            
            # Appending Lex response to response_messages
            response_messages.append({
                "type": "unstructured",
                "unstructured": {
                    "text": response_text,
                }
            })
        
        # Returning response
        return {
            'statusCode': 200,
            'messages': response_messages
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'messages': str(e)
        }
