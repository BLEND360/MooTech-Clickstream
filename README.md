<h1 style="border-bottom: none">DE Capstone Project - Team MooTech</h1>


![Python v3.11](https://img.shields.io/badge/python-v3.11-blue)

<br>

## Problem Statement

The project is designed to perform clickstream analysis for an e-commerce website. The tasks included in this analysis are listed below.
* Getting data from API and storing in S3 bucket
* Ingesting the data, cleaning the data, storing the data using the medallion structure
* Performing transformations to get desired reports

<br>



## Running the code

Once you have setup a databricks workspace, you can follow these steps -
1) Clone this repository into the workspace using the "repo" tab. 
2) Move to the `src` folder.
3) Open `main.py`
4) Assuming you have a cluster setup on databricks, you can connect this python script to a cluster and run the code.

If you're having issues creating a cluster, you can find more information [here](https://docs.databricks.com/clusters/configure.html)

<br>

## Process
The code flow is as follows:
1) Update bronze layer (named raw_data) stored in S3 Bucket by calling API.
2) Update silver layer by performing basic transformations on the transactions table and store it in S3 partitioned by day.
3) Perform transformations to calculate the month over month sales for specified items.
4) Store the sales report in the gold layer (named results) in S3 bucket.

<br>

## Medallion Structure
### Bronze Layer

#### Data Ingestion
Data ingestion can be performed using the ```Data Ingest``` class. An API key should be passed while instanciating the class - 
```python
ingestor = DataIngest(api_key = <api key>)
``` 
Once an object has been created, the method ```get_data_by_range()``` can be used to ingest the required data. The method takes two arguments: 
1) table - which data you want to ingest. Possible values are ```clickstream```, ```transactions```, ```users``` and ```products```.
2) start_date - the start date of the range of data you want.
3) end_date - the end date of the range of data you want. The default value is 'yesterday'.

The ```start_date``` and ```end_date``` arguments must be datetime objects. The function pushes the fetch task into a queue.

```python
ingestor.get_data_by_range(table="clickstream", start_date=<date>, end_date=<date>)
```

Once all data fetches have been defined, you can call ```run_fetch()``` method to hit the API endpoint using the job queue.
```python
ingestor.run_fetch()
```
This will go through the job queue to get all required data and store it in S3 bucket.

#### Quality checks
Quality assurance is performed on the data in a temporary "staging" layer. This QA can be performed using the ```qa_driver()``` which calls the ```schema_check()``` and ```quality_assurance_clickstream()``` functions present in ```utils.data_quality``` module.

Once QA is done, the data is written to the bronze layer.

<br>

### Silver Layer
The silver layer stores the transactions table with the 'timestamp' column being cast to a timestamp data type. We also add an additional column 'last_modified', which is the date the table in silver layer was last updated. This is then written to the silver layer while partitioning by date.

<br>

### Gold Layer
The final Month-over-Month sales report is stored in the gold layer. The report is generated for the product specified in ```main.py``` when calling the ```generate_report()``` function.

