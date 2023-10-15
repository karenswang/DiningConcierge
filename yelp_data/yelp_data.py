import csv 
import requests
import time
import os
from dotenv import load_dotenv
import json

# Yelp API Key
load_dotenv()
api_key = os.getenv('YELP_API_KEY')

# Yelp API endpoint
url = "https://api.yelp.com/v3/businesses/search"

# Headers to authenticate the API call
headers = {
    'Authorization': f'Bearer {api_key}',
    'Accept': 'application/json'
}

# Parameters for the API call
cuisines = ['korean', 'chinese', 'italian', 'mexican', 'thai', 'japanese']
params = {
    'term': 'restaurant',
    'location': 'manhattan',
    'limit': 50  # Yelp only allows 50 businesses per call
}

# Use a dictionary to store restaurants data to avoid duplicates
restaurants_data = {}

# Loop through each cuisine
for cuisine in cuisines:
    print(f"Fetching {cuisine} restaurants...")

    # Update parameters with current cuisine
    params = {**params, 'categories': cuisine}

    # Fetch restaurants for the current cuisine
    for offset in range(0, 1000, 50):
        params['offset'] = offset

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            fetched_restaurants = response.json()['businesses']
            # Pause slightly between API calls to avoid hitting rate limits
            time.sleep(1)
            
            # Add fetched restaurants data along with cuisine type to restaurants_data
            for restaurant in fetched_restaurants:
                business_id = restaurant['id']
                restaurants_data[business_id] = {
                    'business_id': business_id,
                    'name': restaurant['name'],
                    'address': restaurant['location']['address1'],
                    'latitude': restaurant['coordinates']['latitude'],
                    'longitude': restaurant['coordinates']['longitude'],
                    'num_reviews': restaurant['review_count'],
                    'rating': restaurant['rating'],
                    'zip_code': restaurant['location']['zip_code'],
                    'cuisine': cuisine  # directly use the cuisine type used for searching
                } 
        else:
            print(f"Failed to fetch data: {response.status_code}")
            print(response.text)
            break

# Output the total number of fetched restaurants
print(f"Fetched {len(restaurants_data)} restaurants.")

# Prepare data for dynamodb
json_file = "restaurants_dynamodb.json"
# write the JSON data to a file
try:
    with open(json_file, 'w', encoding='utf-8') as jfile:
        json.dump(list(restaurants_data.values()), jfile, ensure_ascii=False, indent=4)
except IOError:
    print("I/O error")
    
# Prepare data for OpenSearch
opensearch_data_lines = []

# Extracting values from restaurants_data, which contains unique restaurant entries
for restaurant in restaurants_data.values():
    # Adding index metadata for OpenSearch
    index_metadata = {
        "index": {
            "_index": "restaurants",
            "_id": restaurant['business_id']
        }
    }
    opensearch_data_lines.append(json.dumps(index_metadata))
    
    # Adding partial restaurant data (only RestaurantID and Cuisine)
    partial_restaurant_data = {
        "RestaurantID": restaurant['business_id'],
        "Cuisine": restaurant['cuisine']
    }
    opensearch_data_lines.append(json.dumps(partial_restaurant_data))

# Define JSON file name for OpenSearch data
json_file_opensearch = "restaurants_opensearch.json"

# Try to write the NDJSON data for OpenSearch to a file
try:
    with open(json_file_opensearch, 'w', encoding='utf-8') as jfile:
        jfile.write('\n'.join(opensearch_data_lines) + '\n')
except IOError:
    print("I/O error for writing OpenSearch data")
