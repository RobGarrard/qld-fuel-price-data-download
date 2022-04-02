################################################################################
'''

                           Backfill Qld Fuel Price Data

'''
################################################################################

# Process all historical data for QLD fuel prices.

# There are two data sources we use here: 

# 1. Qld government open data portal keeps fuel price databases going back to
# 2018, when the fuel price reporting was initiated. These tables can be found
# here: https://www.data.qld.gov.au/dataset/fuel-price-reporting/. They're
# split up by month, and can't seem to be queried all at once. Each month seems
# to have its own resource id, which then can be queried with an API.
# Unfortunately, I haven't been able to find a list of the resource ids, so
# these need to be inferred from some Beautiful Soup logic.

# 2. I have an AWS Lambda set up to query the price data every half hour and
# dump the raw output into S3. At the moment that seems to be using up about
# 1GB of storage a month, since there's lots of duplicated data; so I'm looking
# to replace that fully with the spark streaming script soon. We'll keep it for
# now just in case.

# The strategy for processing the data is to make the two data sets conformable
# to a union, union them, drop duplicates, partition by year and date, and
# write out.

# Since I haven't figured out to to dramatically speed up the Spark/S3
# read/write just yet, we'll have to sync our `raw-data/` location with a
# directory locally (`../../tmp/`). Run the backfill script on that local data,
# write out to that tmp directory, then sync it back with S3.

# Libraries 
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.window import Window


import requests
import json
import os

from bs4 import BeautifulSoup
from urllib.request import Request, urlopen


import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler()) # Log to sterr


################################################################################
# Global parameters

# Local Paths
RAW_DATA_PATH = "../../tmp/raw-data/"
HISTORICAL_DATA_PATH = "../../tmp/historical-data/"
OUTPUT_PATH = '../../tmp/processed-data/'

# QLD Fuel Price API Token
API_TOKEN = 'XXXX-XXXX-XXXX-XXXX'

# QLD fuel price API URL
API_URL = 'https://fppdirectapi-prod.fuelpricesqld.com.au'

# Header that must be passed to the HTTP request when accessing fuel price API.
FUEL_API_AUTH = f'FPDAPI SubscriberToken={API_TOKEN}'

################################################################################


def pull_historical_data(spark):
    '''
    Pull all the historical fuel price data we can from the QLD data portal.

    These data sets are split up by month, so we need to infer the resource_id
    for each month, and load it separately.
    '''

    ############################################################################
    # Load the page and process it with BeautifulSoup

    req = Request(
        "https://www.data.qld.gov.au/dataset/fuel-price-reporting"    
        )
    html_page = urlopen(req)
    soup = BeautifulSoup(html_page, "lxml")

    ############################################################################
    # Find all the resource_ids in the page. The text for the links we want are
    # of the form "Queensland Fuel Prices March 2022", so find the links with
    # that prefix.

    links = {}
    for link in soup.findAll('a'):
        
        text = link.text.lstrip()
        href = link.get('href')
        
        prefix = "Queensland Fuel"
        if text.startswith(prefix):
            
            # I can't quite remember why I do this check...
            try:
                resource_id = href.split('?')[0].split('/')[4]
                links[text] = resource_id
            except:
                print('Passing on: ', text)
                pass

    ########################################################################### 
    # Pull data and create data frame
    
    # Can't seem to create a Spark empty data frame for unioning like you can in
    # Pandas. Initialize a None, then replace it with a data frame on the first
    # try.
    historical_df = None

    for text, resource in links.items(): 
        
        # Query all the data in this resource. The largest data set I've seen
        # has around 120,000 observations; so set the limit high. If we don't
        # specify a limit, it will default to limit=100.
        query = (
            f'https://www.data.qld.gov.au/api/3/action/'
            f'datastore_search?resource_id={resource}&limit=500000'
            )
        
        response = requests.get(query)
        logger.info(f'Dataset: {text}')
        logger.info(f'Status: {response.status_code}')
        
        if response.status_code == 200:
            
            # Load json payload
            data = json.loads(response.text)['result']['records']

            # Convert to spark data frame
            data = spark.createDataFrame(data)
            
            # Some of the earlier databases have 'Fuel Price' rather
            # than 'fuel_price'. Lowercase the columns and replace spaces with
            # underscores.
            for col in data.columns:
                data = (
                    data
                    .withColumnRenamed(col,
                                       col.lower().replace(' ', '_')
                                       )
                    )
        
            if historical_df is None:
                historical_df = data
            else:
                historical_df = historical_df.unionByName(data)
                
    ############################################################################
    # Process names
    column_mapping = ({
        'siteid':'site_id',
        'site_brand': 'brand',
        'sites_address_line_1': 'site_address',
        'site_post_code': 'postcode',
        'site_latitude': 'lat',
        'site_longitude': 'long',
        'transactiondateutc': 'timestamp'
        })

    for key, value in column_mapping.items():
        historical_df = (
            historical_df
            .withColumnRenamed(key, value)
        )

    # Drop unused columns
    historical_df = (
        historical_df
        .drop('_id', 'site_state', 'site_suburb')
        .withColumn('date', f.to_date('timestamp'))
        .withColumn('year', f.year('date'))
        .withColumn('collection_method', f.lit(None).cast('string'))
        .withColumn('price', f.col('price').cast('double'))
    )

    # Reorder columns
    historical_df = historical_df.select(sorted(historical_df.columns))

    ############################################################################

    # Write out
    logger.info(f'Writing historical data out to {HISTORICAL_DATA_PATH}.')
    
    (
        historical_df
        .repartition('year', 'date')
        .write
        .mode('overwrite')
        .partitionBy('year', 'date')
        .parquet(HISTORICAL_DATA_PATH)
    )

    return None


################################################################################
# Main method

def main(spark):
    '''
    Process json files in raw-data path. These contain price data. Drop
    duplicates and join them with non-price data. Write to S3 partitioned by
    date.
    '''
    
    ############################################################################
    # Read in price data

    logger.info('Processing price data.')

    price_df = (
        spark 
        .read
        .option("recursiveFileLookup", "true")
        .json(RAW_DATA_PATH + 'prices/')
        )
    
    price_df = (
        price_df
        .withColumn('SitePrices', f.explode('SitePrices'))
        .select('SitePrices.*')
        .withColumnRenamed('TransactionDateUtc', 'timestamp')
        .withColumnRenamed('CollectionMethod', 'collection_method')
        .withColumnRenamed('FuelId', 'fuel_id')
        .withColumnRenamed('Price', 'price')
        .withColumnRenamed('SiteId', 'site_id')
        .withColumn('date', f.to_date('timestamp'))
        .withColumn('year', f.year('date'))
        .dropDuplicates()
        )

    ############################################################################
    # Read in fuel data

    logger.info('Processing fuel type data.')


    fuel_type_df = (
        spark
        .read
        .json(RAW_DATA_PATH + 'fuel-types/fuel_types.json')    
        )

    fuel_type_df = (
        fuel_type_df
        .withColumn('Fuels', f.explode('Fuels'))
        .select('Fuels.*')
        .withColumnRenamed('FuelId', 'fuel_id')
        .withColumnRenamed('Name', 'fuel_type')
        )

    ############################################################################
    # Read in brand data
    
    logger.info('Processing brand data.')

    brand_df = (
        spark
        .read
        .json(RAW_DATA_PATH + 'brands/brands.json')    
        )

    brand_df = (
        brand_df
        .withColumn('Brands', f.explode('Brands'))
        .select('Brands.*')
        .withColumnRenamed('BrandId', 'brand_id')
        .withColumnRenamed('Name', 'brand')
        )

    ############################################################################
    # Read in site data

    logger.info('Processing site data.')

    site_df = (
        spark
        .read
        .json(RAW_DATA_PATH + 'sites/sites.json')    
        )

    # They haven't named the site columns very well.
    site_rename_map = ({
        'S': 'site_id',
        'A': 'site_address',
        'N': 'site_name',
        'B': 'brand_id',
        'P': 'postcode',
        'Lat': 'lat',
        'Lng': 'long',
    })
    
    site_df = (
        site_df
        .withColumn('S', f.explode('S'))
        .select('S.*')
        .select(
                [f.col(c).alias(new_name)
                 for c, new_name in site_rename_map.items()
                ]
               )
        )
    
    ############################################################################
    # Join non-price and price data

    logger.info('Joining price, fuel, brand, and site.')
    df = (
        price_df
        .join(site_df,
              on='site_id', 
              how='left',
              )
        .join(fuel_type_df,
              on='fuel_id',
              how='left',
              )
        .join(brand_df,
              on='brand_id',
              how='left',
              )
        .drop('fuel_id', 'brand_id')
        )

    ############################################################################
    # Join historical data
    
    historical_df = (
        spark
        .read
        .parquet(HISTORICAL_DATA_PATH)
    )
    


    ############################################################################
    # Final transformations

    logger.info('Final transformations.')

    df = (
        df
        .unionByName(historical_df)
        .dropDuplicates()
        .withColumn('price', f.col('price') / 10)
        .dropna(subset=['year', 'date'])
        )
    df = df.select(sorted(df.columns))
    
    ############################################################################
    # Include only changes in price. It seems as if the bigger companies have an
    # automated system to post their prices to the QLD Fuel Price registry at
    # the same time each day. Often these will not be changes from the previous
    # price.

    # Strategy: Create a window partitioned by SiteID and Fuel Type, order it by
    # time. Construct a first difference in price column over that window. If
    # the price delta is 0, then that observation corresponds to an entry with
    # no price change; so drop it.

    logger.info("Dropping entries where price hasn't changed.")

    # Window columns are everything excep price and timestamp.
    window_cols = list(set(df.columns) - {'price', 'timestamp'})
    window = Window.partitionBy(window_cols).orderBy('timestamp')

    df = (
        df
        .withColumn('price_delta', 
                    f.col('price') - f.lag(f.col('price'), 1, 0).over(window)
                   )
        .filter(f.col('price_delta') != 0)
        .drop('price_delta')
    )

    ############################################################################
    # Write to local directory
    
    logger.info('Write out to local file system.')
    
    (
        df
        .repartition('year', 'date')
        .write
        .mode('overwrite')
        .partitionBy('year', 'date')
        .parquet(OUTPUT_PATH)
    )

    return None


if __name__ == '__main__':

    # Start a spark session
    spark = (
        SparkSession
        .builder
        .config('spark.app.name', 'Backfill QLD Fuel Prices')
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .getOrCreate()
        )

    # Try to run the main method, if it fails catch the error to stop the spark
    # session.
    try:
        # First check if historical data has already been downloaded. If it has,
        # skip the download.
        logger.info('Checking for historical data.')
        if os.path.isdir(HISTORICAL_DATA_PATH):
            logger.info('Previous historical data found. Skipping download.')
        else:
            logger.info('Downloading historical data.')
            pull_historical_data(spark)


        logger.info('Processing data.')
        main(spark)

    except Exception as exception:
        logger.info('Error: stopping spark.')
        spark.stop()
        raise exception

    logger.info("Completed successfully.")
    spark.stop()

