################################################################################
'''

AWS Lambda function to pull fuel price data from QLD fuel price API 
(https://fuelpricesqld.com.au/). Stores this data on AWS S3.

The HTTP requests run here requires an authorization token. Obtain a token by
signing up at the bottom of the home page. You should receive an email with
your token within 2 days. 

Replace `MY_TOKEN` below with your token. Replace `BUCKET` with your bucket.
Remember to give the lambda permission to access your bucket.

Prices should be pulled half-hourly.

'''
################################################################################
# Libraries

import requests
import json
import datetime
import boto3

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

################################################################################
# Global Parameters

# Auth token
# Replace this with your own
MY_TOKEN = "XXXX-XXXX-XXXX-XXXX"

# HTTP request parameters
URL = "https://fppdirectapi-prod.fuelpricesqld.com.au"
FUEL_API_AUTH = f'FPDAPI SubscriberToken={MY_TOKEN}'

# Output path
BUCKET = 'qld-fuel-prices'

################################################################################
# Data Download

def pull_data():
    '''
    Query the API for fuel prices and save the output to S3.
    '''

    # Timestamp for file name.
    TODAYS_DATE = (
        datetime
        .datetime
        .now(datetime.timezone.utc)
        .strftime("%Y-%m-%d")
        )
    NOW_TIMESTAMP = (
        int(
            datetime
            .datetime
            .now(datetime.timezone.utc)
            .timestamp()
            )
        )

    logger.info("Retrieving price data.")
    
    query = (
        f"{URL}/Price/GetSitesPrices?countryId=21&"
         "geoRegionLevel=3&geoRegionId=1"
    )
    response = requests.get(query, headers={'Authorization': FUEL_API_AUTH})
    
    payload = json.loads(response.text)
    
    logger.info("Complete.")

    ############################################################################
    # Write to S3

    logger.info("Writing data to S3.")
    
    KEY = (
        f'raw-data/prices/{TODAYS_DATE}/'
        f'prices_{TODAYS_DATE}_{NOW_TIMESTAMP}.json'
    )
    logger.info(f"File name: {KEY}")
    
    s3 = boto3.client('s3')
    s3.put_object(
     Body=json.dumps(payload),
     Bucket=BUCKET,
     Key=KEY,
    )

    logger.info("Complete.")
    
    return None


################################################################################
# Handler

def lambda_handler(event, context):
    
    # Try to pull data
    try:
        pull_data()
        response = {
            "statusCode": 200,
            "body": json.dumps({
                "status": "Success",
            })
        }
            
    except Exception as exception:
        logger.error(exception)
        response = {
            "statusCode": 400,
            "body": json.dumps({
                "status": "Failed",
            })
        }
    
    return response
    

################################################################################

if __name__ == '__main__':
    response = lambda_handler(None, None)
    print(response)