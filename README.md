
<h1 style="border-bottom: none">DE Capstone Project - Team MooTech</h1>


![Python v3.9](https://img.shields.io/badge/python-v3.9-blue)

<br>

## Problem Statement

The project is designed to perform clickstream analysis for an e-commerce website. The tasks included in this analysis are listed below.
* Getting data from API and storing in S3 bucket
* Ingesting the data, cleaning the data, storing the data using the medallion structure
* Performing transformations to get desired reports

<br>
<br>

## Getting Started

The project is designed to be run on databricks. 

### Requirements

#### pyspark
- you can install pyspark to your local machine using `pip install pyspark` in your terminal

### Running the code 
Once you have setup a databricks workspace, you can follow these steps -
1) Clone this repository into the workspace using the "repo" tab. 
2) Move to the `src` folder.
3) Open `main.py`
4) Assuming you have a cluster setup on databricks, you can connect this python script to a cluster and run the code.

If you're having issues creating a cluster, you can find more information [here](https://docs.databricks.com/clusters/configure.html)

### Process
The code flow is as follows.
1) Update bronze layer (named raw_data) stored in S3 Bucket by calling API.
2) Update silver layer by performing basic transformations on the transactions table and store it in S3 partitioned by day.
3) Perform transformations to calculate the month over month sales for specified items.
4) Store the sales report in the gold layer (named results) in S3 bucket.

### Expected Output
The code calculates the month over month sales report for a specific product. This sales report is stored in the gold layer (named results) in the s3 bucket.
