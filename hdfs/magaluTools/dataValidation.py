from abc import ABC, abstractmethod
from typing import Optional
from magaluTools.logger import get_logger
from pyspark.sql.functions import col, isnan
import pyspark.sql.dataframe as DataFrame


class Validator(ABC):
    """
        Abstract Class for the df column validation strategies
    """

    @classmethod
    @abstractmethod
    def validate(cls, df: DataFrame, col_to_check: str) -> bool:
        """

        Args:
            df: The spark dataframe to be checked
            col_to_check: The column name that the validator will apply the validation strategy

        Returns:
            True - If the validation has passed
            False - If the validation has failed
        """
        ...


class NullValidator(Validator):
    """
        Strategy to check if there are any null like data in the df
    """
    @classmethod
    def validate(cls, df: DataFrame, col_to_check: str) -> bool:
        null_count = df.filter((df[col_to_check] == "") | df[col_to_check].isNull() | isnan(df[col_to_check])).count()
        try:
            assert null_count == 0
            get_logger().info(f'[DATA VALIDATION] Nullity Check for column {col_to_check}. RESULT: PASSED')
            return True
        except AssertionError:
            get_logger().warning(f'[DATA VALIDATION] Nullity Check for column {col_to_check}. RESULT: FAILED')
            return False


class UniqueKeyValidator(Validator):
    """
        Strategy to check if there are duplicated data in the specified column
    """
    @classmethod
    def validate(cls, df: DataFrame, col_to_check: str) -> bool:
        try:
            assert df.groupBy(col(col_to_check)).count().filter(col('count') > 1).count() == 0
            get_logger().info(f'[DATA VALIDATION] Unique Key Check for column {col_to_check}. RESULT: PASSED')
            return True
        except AssertionError:
            get_logger().warning(f'[DATA VALIDATION] Unique Key Check for column {col_to_check}. RESULT: FAILED')
            return False


class MagaluValidator:
    """
        Summary class that can be used to assemble the strategies to be ran for the dataset and its respective columns
    """
    def __init__(self):
        self.__validations: dict = {}
        self.__df: Optional[DataFrame] = None
        self.__result: dict = {}

    def setDfToValidate(self, df: DataFrame):
        self.__df = df
        return self

    def setValidation(self, df_column: str, validator: Validator):
        self.__validations.update({df_column: validator})
        return self

    def validate(self, raise_if_failed: bool = False) -> dict:
        if not self.__validations:
            get_logger().info('No validations set')
            return self.__result

        for df_column, validator in self.__validations.items():
            validation_result = validator.validate(self.__df, df_column)

            self.__result.update({
                'column': df_column,
                'validator': validator,
                'result': validation_result
            })

            try:
                assert validation_result
            except AssertionError as e:
                if raise_if_failed:
                    raise e(f'One df Validation has failed! Column: {df_column} | Validation: {validator.__name__}')

        return self.__result
