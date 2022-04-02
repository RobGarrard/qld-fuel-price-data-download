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
    s3 = boto3.client('s3')

    ############################################################################
    # Fuel Types

    logger.info("Retrieving fuel type data.")

    # Query API
    query = f'{URL}/Subscriber/GetCountryFuelTypes?countryId=21'
    response = requests.get(query, headers={'Authorization': FUEL_API_AUTH})
    payload = json.loads(response.text)

    # Write to S3
    KEY = 'raw-data/fuel-types/fuel_types.json'
    
    s3.put_object(
     Body=json.dumps(payload),
     Bucket=BUCKET,
     Key=KEY,
    )

    logger.info("Complete.")
        
    ############################################################################
    # Brands

    logger.info("Retrieving brand data.")
    
    query = f'{URL}/Subscriber/GetCountryBrands?countryId=21'
    response = requests.get(query, headers={'Authorization': FUEL_API_AUTH})
    payload = json.loads(response.text)

    # Write to S3
    KEY = 'raw-data/brands/brands.json'
    
    s3.put_object(
     Body=json.dumps(payload),
     Bucket=BUCKET,
     Key=KEY,
    )
    
    logger.info("Complete.")

    ###########################################################################
    # Site Details

    logger.info("Retrieving site details data.")

    query = (
        f'{URL}/Subscriber/GetFullSiteDetails?countryId=21&'
        'geoRegionLevel=3&geoRegionId=1'
    )
    response = requests.get(query, headers={'Authorization': FUEL_API_AUTH})
    payload = json.loads(response.text)
    
    # Write to S3
    KEY = 'raw-data/sites/sites.json'
    
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