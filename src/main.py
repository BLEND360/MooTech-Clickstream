from data_ingestion import DataIngest
from src.Transformations.month_over_month import MoMTransformer
from src.Transformations.setup_sliver_layer import SilverLayer
from src.utils.common_functions import get_latest_transaction_date, save_to_gold_layer
from utils.load_tables import *
from utils.data_quality import *
import datetime


def update_bronze_layer():
    """
    initializes the DataIngest Class and retrieves the data since last load
    for transaction tables and the latest for SCD tables
    """
    # api_key = dbutils.secrets.get(scope='mootech-scope', key='mootech-key')
    api_key = dbutils.secrets.get(scope='my-scope-1', key='api-key')

    # update transactions tables
    data_ingest = DataIngest(api_key=api_key)
    start_date = get_latest_transaction_date()

    data_ingest.get_data_by_range(table='clickstream', start_date=start_date)  # COME UP WITH AN SLA PERIOD

    data_ingest.get_data_by_range(table='transactions', start_date=start_date)
    # update SCD tables
    day_before_yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=2)
    yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)

    data_ingest.get_data_by_range(table='users',
                                  start_date=day_before_yesterday,
                                  end_date=yesterday,
                                  )

    data_ingest.get_data_by_range(table='products',
                                  start_date=day_before_yesterday,
                                  end_date=yesterday,
                                  )
    data_ingest.run_fetch()
    qa_driver("clickstream")
    clickstream_bronze_df = get_clickstream(path=S3Layers.STAGE.value)
    clickstream_bronze_df.write.mode("overwrite").partitionBy("utc_date").format("delta").save(
        S3Layers.BRONZE_TEST.value + "/clickstream")
    print('bronze layer updated successfully')


def update_silver_layer():
    # setup and save data into silver layer
    silver_layer = SilverLayer()
    silver_layer.setup_transactions()
    silver_layer.save_transactions()


def update_gold_layer(sales_report, filename):
    # save report to gold layer
    save_to_gold_layer(sales_report, f"sales_report_{filename}")
    print('report saved to gold layer')


def generate_report(item_to_be_queried: str):
    # get transformer
    transformer = MoMTransformer()

    # get month_over_month_report
    sales_report = transformer.transform(item_to_be_queried)
    print('report generated successfully')
    return sales_report


def qa_driver(table_name: str):
    if table_name == "clickstream":
        table_stage = get_clickstream(path=S3Layers.STAGE.value)
        table_bronze = get_clickstream(path=S3Layers.BRONZE.value)
        schema_check(table_bronze, table_stage)
        quality_assurance_clickstream(table_stage)


def main():
    """
    Ingests data and saves to bronze layer
    Sets up Silver layer with transactions table
    Calculates month-over-month sales report for item
    :return: None
    """

    # Ingest new data
    update_bronze_layer()

    # update silver layer
    update_silver_layer()

    # generate and save report to gold layer
    sales_report = generate_report('tumbler')
    update_gold_layer(sales_report, 'tumbler')


if __name__ == "__main__":
    main()
