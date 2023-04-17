from src.utils import load_tables
from src.utils.S3Layers import S3Layers
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, from_unixtime, to_timestamp, dayofweek

spark = SparkSession.builder.getOrCreate()


class SilverLayer:

    def __init__(self):
        self.transactions_df = None
        # self.setup_transactions()

    def save_transactions(self, mode: str = 'overwrite'):
        current_timestamp = from_unixtime(unix_timestamp(), 'yyyy-MM-dd')

        (
            self.transactions_df
            .select(
                '*',
                dayofweek(col('utc_date')).alias('day_of_week'),
                current_timestamp.alias('last_modified')
            )
            .orderBy('order_id')
            .write
            .format('delta')
            .mode(mode)
            .partitionBy('day_of_week')
            .save(f"{S3Layers.SILVER.value}/transactions")
        )
        print('transactions saved to silver layer')

    def setup_transactions(self):
        try:
            self.transactions_df = (
                load_tables.get_transactions()
                .select(
                    'order_id',
                    'email',
                    'transaction_type',
                    'items',
                    'total_item_quantity',
                    'total_purchase_usd',
                    to_timestamp('transaction_timestamp').alias('timestamp'),
                    'utc_date'
                )
            )

            # self.save_transactions()
        except Exception as e:
            print('SILVER LAYER')
            print(e)
