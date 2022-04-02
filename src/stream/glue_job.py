################################################################################
'''
                            Qld Fuel Price Stream
'''
################################################################################
# This job checks for new data in `s3://{BUCKET}/raw-data/prices/`. If there is
# data for today that hasn't been processed into parquet yet, it will process
# that data and write it out to `s3://{BUCKET}/processed-data/`.


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import pyspark.sql.functions as f

import json
import datetime 

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler()) # Log to sterr

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

################################################################################
# Global Parameters
BUCKET = 'qld-fuel-prices'

# Date at runtime
todays_date = (
    datetime
    .datetime
    .now(datetime.timezone.utc)
    .strftime("%Y-%m-%d")
    )


# Input files
price_input = f's3a://{BUCKET}/raw-data/prices/{todays_date}/'
fuel_type_input = f's3a://{BUCKET}/raw-data/fuel-types/fuel_types.json'
brand_input = f's3a://{BUCKET}/raw-data/brands/brands.json'
site_input = f's3a://{BUCKET}/raw-data/sites/sites.json'

# Output
output_path = f's3a://{BUCKET}/processed-data/'

def main():

    ################################################################################
    # Read in price data
    
    logger.info('Processing price data.')
    
    # If no data has been dumped into a directory for today,(e.g., if it's around
    # midnight UTC and the lambda hasn't run yet),this {todays_date} dir won't exist
    # and so we'll get an error. Catch it.
    
    try:
        price_df = (
            spark 
            .read
            .option("recursiveFileLookup", "true")
            .json(price_input)
            )
    except Exception as exception:
        logger.info('No data for today.')
        return None
        
    
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
        .filter(f.col('date') == todays_date)
        .dropDuplicates()
        )
    
    ############################################################################
    # Read in fuel data
    
    logger.info('Processing fuel type data.')
    
    fuel_type_df = (
        spark
        .read
        .json(fuel_type_input)    
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
        .json(brand_input)    
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
        .json(site_input)    
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
    # Final transformations
    
    logger.info('Final transformations.')
    
    df = (
        df
        .dropDuplicates()
        .withColumn('price', f.col('price') / 10)
        )
    df = df.select(sorted(df.columns))
    
    ############################################################################
    # Read in current data
    
    current_data = (
        spark
        .read
        .parquet(output_path)
        )
    
    current_data_size = current_data.count()
    logger.info(f'Current data size: {current_data_size}')
    
    ############################################################################
    # Union new data to current. Only write out if there is now more data.
    
    df = (
        df
        .unionByName(current_data)
        .dropDuplicates()
        .dropna(subset=['year', 'date'])
        .filter(f.col('date') == todays_date)
        )
    
    updated_data_size = df.count()
    logger.info(f'Updated data size: {updated_data_size}')
    
    if updated_data_size > current_data_size:
        logger.info('New data found. Writing to S3.')
    
        # Set partition overwrite mode to dynamic so that we don't delete the
        # rest of our partitions.
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    
        (
            df
            .repartition('year', 'date')
            .write
            .mode('overwrite')
            .partitionBy('year', 'date')
            .parquet(output_path)
        )
        
    else:
        logger.info('No new data found.')

    return None
    
    
logger.info('Beginning main method.')
main()
logger.info('Script successfully completed.')




