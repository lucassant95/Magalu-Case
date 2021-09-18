from typing import Optional, List
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date
from magaluTools.dataValidation import *


def src2raw(file_path: str, table_name: str, validations: Optional[List[str]] = None, partition_data: Optional[dict] = None,
            csv_delimiter: str = '|', raise_if_validation_failed: bool = False) -> DataFrame:
    """

    Args:
        file_path: The path to the csv file to be dumped in the raw zone of the warehouse
        validations: A list of validations needed to be executed in the data from the csv files
        partition_data: The metadata responsible for creating the data partition correctly
            (expected format: {'id_categoria': ['unique', 'null']}) - Available validations: 'null', 'unique'
        table_name: The table name to be saved in the warehouse
        csv_delimiter: The delimiter character used in the csv file to be dumped
        raise_if_validation_failed: Raises an AssertionException if any validation has failed

    Returns:

        The dataframe that was created based on the csv file
    """

    # Gets the current Spark session being used
    spark = SparkSession.getActiveSession()

    # Reads the csv file
    df = spark.read.option("header", "true"
                            ).format('csv'
                            ).option('path', file_path
                            ).option('delimiter', csv_delimiter
                            ).load()

    # If any validations are provided, validates the data
    if validations:
        validator = MagaluValidator()
        validator.setDfToValidate(df)
        for column, validations_list in validations.items():
            if 'null' in validations_list: validator.setValidation(column, NullValidator)
            if 'unique' in validations_list: validator.setValidation(column, UniqueKeyValidator)
        validator.validate(raise_if_validation_failed)

    # If any partition metadata is present, partitions the dataframe according to the parameters
    if partition_data:
        target = col(partition_data.get('column')).cast('string')
        df = df.withColumn('partition', to_date(target) if partition_data.get('type') == 'date' else target)

        df.write.partitionBy('partition').format('parquet').mode('overwrite').saveAsTable(f'raw.{table_name}')
        return df

    df.write.format('parquet').mode('append').saveAsTable(f'raw.{table_name}')
    return df
