from enum import Enum

class S3Layers(Enum):
    """
    Maps medallion data location to variables
    so that calls are constant
    """
    STAGE = "s3://allstar-training-mootech/stage"
    BRONZE_TEST = "s3://allstar-training-mootech/bronze_test"
    BRONZE = "s3://allstar-training-mootech/raw_data"
    SILVER = "s3://allstar-training-mootech/silver_layer"
    GOLD = "s3://allstar-training-mootech/results"
    SILVER_TEMP = "s3://allstar-training-mootech/silver_temp"