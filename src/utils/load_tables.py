"""
    This file takes data present in S3 bucket
    and creates data frames using spark object

    Functions:
    - get_transactions_raw: loads transaction table from bronze layer
    - get_users: loads users table from bronze layer. (To be implemented)
    - get_products: loads products table from bronze layer
    - get_clickstream: loads click stream table from bronze layer. (To be implemented)
"""

from pyspark.sql import SparkSession
from src.utils.S3Layers import S3Layers

spark = SparkSession.builder.getOrCreate()


def get_transactions(path: str = S3Layers.BRONZE.value, data_format: str = "parquet"):
    """
    Returns the raw transactions data stored in bronze layer
    :param data_format: which format the stored data uses. defaults to parquet
    :param path: the path to the bronze layer
    :return: transactions data frame
    """

    if 'raw_data' in path:
        ext = "/**"
    else:
        ext = ""

    transactions_df = (spark
                       .read
                       .format(data_format)
                       .load(f"{path}/transactions{ext}")
                       )

    return transactions_df


def get_products(path: str = S3Layers.BRONZE.value, data_format: str = "delta"):
    """
    Returns the raw products table stored in bronze layer
    :param data_format:  which format the stored data uses. defaults to delta
    :param path: the path to bronze layer
    :return: products data frame
    """

    products_df = (spark
                   .read
                   .format(data_format)
                   .load(f"{path}/products")
                   )
    return products_df


def _create_name_map():
    """
    Creates a map for product id and product name
    :return: Spark dataframe with name to product id mapping
    """
    data = [
        ('product0', 'tumbler'),
        ('product1', 'Sweater'),
        ('product2', 'Beanie'),
        ('product3', 'Mug'),
        ('product4', 'Quarter-zip'),
        ('product5', 'Power-bank'),
        ('product6', 'Pen'),
        ('product7', 'Sticker'),
        ('product8', 'Keychain'),
        ('product9', 'Coaster')
    ]

    headers = ['product_id', 'product_name']

    return spark.createDataFrame(data, headers)


name_map = _create_name_map()
