import pandas as pd
import numpy as np
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, StringType
from pyspark.sql.functions import col, countDistinct, when, isnan, isnull, count,  udf, lit
from datetime import datetime
import requests
import json

def schema_check(df, df2):

    columns = df.columns
    columns2 = df2.columns

    difference = columns.difference(columns2)

    if difference.isempty():
        print('Schema Unchanged')
    else:
        print('Schema Changed')

def quality_assurance_clickstream(df):
    # Check for missing values
    print("Missing Values:")
    missing_values = df.select([count(when(col(column).isNull(), column)).alias(column) for column in df.columns])
    missing_values.show()

    # Check for duplicate records
    print("\nDuplicate Records:")
    duplicates = df.count() - df.dropDuplicates().count()
    print(duplicates)
    print('Dropping Duplicates')
    df = df.dropDuplicates()

    # Check data types
    print("\nData Types:")
    data_types = df.dtypes
    print(data_types)

    # Inspect unique values for categorical columns
    print("\nUnique Values for Categorical Columns:")
    categorical_columns = ['campaign', 'currency', 'os', 'pagename']
    for column in categorical_columns:
        unique_values = df.select(column).distinct().collect()
        print(f"{column}: {unique_values}")

    # Check for inconsistencies in date columns
    print("\nDate Columns:")
    date_columns = ['cust_hit_time_gmt', 'first_hit_time_gmt', 'hit_time_gmt', 'visit_start_time_gmt']
    for column in date_columns:
        min_date = df.agg({column: "min"}).collect()[0][0]
        max_date = df.agg({column: "max"}).collect()[0][0]
        print(f"{column}: {min_date} - {max_date}")


def quality_assurance_transactions(df):
    #implement missing value checks
    missing_values = df.select([count(when(isnan(c) | isNull(c), c)).alias(c) for c in df.columns])
    missing_values.show()

    #implement dupe checks
    duplicates = df.groupBy("order_id").agg(count("*").alias("count")).filter(col("count") > 1)
    duplicates.show()

    # Check for invalid data 
    invalid_data = df.filter(col("price") < 0)
    invalid_data.show()

    #check if the API returns same data everytime
    # pull twice and compare

    #null value counts
    null_counts = df.select([sum(isNull(c)).alias(c) for c in df.columns])
    null_counts.show()

def fetch_validate_data(date, api_type: str):
    url = "https://en44bq5e33.execute-api.us-east-1.amazonaws.com/dev/metrics/"+api_type
    payload = json.dumps({"date": datetime.strftime(date, '%m-%d-%Y')})
    headers = {"Content-Type": "application/json"}

    response = requests.get(url, data=payload, headers=headers)
    if response.status_code == 200:
        return int(response.text)
    return None

def validate_table(original_df):

    # Find the unique visitors for each date
    unique_visitors_df = original_df.groupBy("utc_date").agg(
        countDistinct("visid_high", "visid_low").alias("unique_visitors")
    )

    # Define a UDF to fetch visitor data from the API
    

    # Fetch visitor data from the API for each date and create a new DataFrame
    api_visitors_temp = unique_visitors_df.select('utc_date')
    api_visitors_df = api_visitors_temp.withColumn(
        "api_visitors", fetch_validate_data(col("utc_date"), "visitors")
    )

    # # Join both DataFrames based on the date
    combined_df = unique_visitors_df.join(api_visitors_df, on='utc_date')

    # # Calculate the difference between the two visitor counts and add it as a new column
    final_df = combined_df.withColumn(
        'visitor_difference', col('api_visitors') - col('unique_visitors') 
    )

    # # # # Show the final DataFrame
    final_df.show()

    # Find the unique visitors for each date
    unique_hits_df = original_df.groupBy("utc_date").agg(
        countDistinct("hitid_high", "hitid_low").alias("unique_hits")
    )

    # Fetch visitor data from the API for each date and create a new DataFrame
    api_hits_temp = unique_hits_df.select('utc_date')
    api_hits_df = api_hits_temp.withColumn(
        "api_hits", fetch_validate_data(col("utc_date"), "hits")
    )

    # # Join both DataFrames based on the date
    combined_hits_df = unique_hits_df.join(api_hits_df, on='utc_date')

    # # Calculate the difference between the two visitor counts and add it as a new column
    final_hits_df = combined_hits_df.withColumn(
        'hit_difference', col('api_hits') - col('unique_hits') 
    )

    # # # # Show the final DataFrame
    final_hits_df.show()