from src.utils.S3Layers import S3Layers
import src.utils.load_tables as load_tables
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

spark = SparkSession.builder.getOrCreate()


def get_latest_transaction_date():
    """
    Calculates the latest date in transactions table stored in Bronze layer
    :return: latest date in data
    """
    transactions_df = load_tables.get_transactions(S3Layers.BRONZE.value)

    return transactions_df.agg(max(col('utc_date')).alias('last_date')).collect()[0]['last_date']


def save_to_gold_layer(data, filename: str):
    """
    Saves given dataframe with given filename to gold layer
    :param data: the dataframe that needs to be saved
    :param filename: the filename you want your data stored as
    :return: None
    """
    data.write.mode('overwrite').parquet(f"{S3Layers.GOLD.value}/{filename}")
