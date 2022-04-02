# Queensland Fuel Price Data Download

AWS Lambdas for querying QLD fuel prices.

The [Queensland Fuel Price Reporting Scheme](https://www.fuelpricesqld.com.au/) maintains an API for querying the price of fuel at each petrol station in Qld.

Stations are required to inform the scheme within half an hour of a change in
their posted fuel prices.

This repo generates AWS Lambda functions that query the API and dump data into
an S3 bucket.

Prices need to be queried at near the half-hour frequency. Non-price data
(petrol station data, fuel type mappings, and brand mappings) are relatively
static and don't need to be queried often. I pull them once a day. I've made
two Lambdas, one to query the fuel prices, another to query the non-price
data.


## Getting Started

1\. Sign up as a data consumer for the Qld fuel reporting API: [here](https://forms.office.com/Pages/ResponsePage.aspx?id=XbdJc0AKKUSHYhmf2mnq-9XqCWIciN5Osw2Y74gWzu9UQ0pCR1dPV0FWR1ZPN0FYSEc0UEVQMkQzMyQlQCN0PWcu).
It might take a few days for them to email you your access token.

2\. Clone the repo.

```git clone https://github.com/RobGarrard/qld-fuel-price-data-download.git```

```cd qld-fuel-price-data-download```


3\. Replace `MY_TOKEN = "XXXX-XXXX-XXXX-XXXX"` with your token in each lambda
(`src/price-data-lambda/price-data-lambda.py` and
`src/non-price-data-lambda/non-price-data-lambda.py`). Also replace `BUCKET
= 'qld-fuel-prices'` with the name of your S3 bucket.

4\. Build the Python environment and the lambdas.

```make env```

```make price-lambda```

```make non-price-lambda```

This dumps two zip files into `build/`.

5\. Upload these as two different Lambdas to AWS. Test them and attach an
EventBridge trigger to each. I set the price-lambda to run half-hourly; and the
non-price-lambda to run daily.

6\. The Lambdas with dump data into `s3://{YOUR-BUCKET}/raw-data/` as json
files.

## Spark Processing

The steps above will result in the API being queried at whatever frequency
you've specified and the raw data being dumped into S3. 

First, we need to enrich this data with historical fuel prices, which can't be
pulled directly from the API. We also want to process the data to include only
observations where a price has changed for a station-fuel type combination.
This will reduce the amount of data needing to be transmitted across the
network in whatever application we build.

Second, we need to process the raw data at some daily frequency to put the raw
data pulled by the Lambdas into the same curated data set.

For the former, there is a Spark job in `src/backfill/` that queries the
historical data, all the current output of the Lambdas, and processes these
into parquet partitioned by year and date. Since I'm stupid, I do this by
pulling the raw data from S3 into a temporary directory (using `make
download-raw-data`), running the Spark job, then uploading the output to its
resting place in S3 (using `make upload-processed-data`). I couldn't get my
Spark S3 committer to not be slow as all heck; I know it's not optimal. 


For the latter, there is a AWS Glue job in `src/stream/`. This will check for
new data in the `raw-data` S3 location, process it, and append it to the
processed data. I have a Glue Workflow set up to run this Glue job a couple of
times a day, then trigger a crawler to update my Athena tables.


Contact: [garrard.robert@gmail.com](mailto:garrard.robert@gmail.com)