from helpers.constant.sparkenv import SparkEnv
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from helpers.constant import StoragePath
from helpers.factory import SparkHelper
from helpers.factory import RdbmsEtlFactory
from helpers.factory import DatetimeFactory
from helpers.constant.common import CommonConstant
import argparse
from config import Config
from example.queries import Queries
from pyspark.sql.functions import expr

class Increment():
    def __init__(self,
                 spark: SparkSession,
                 s3_bucket: str,
                 execute_date_time: str,
                 config: Config,
                 interval_days: int = CommonConstant.ONE):
        self.spark = spark
        self.s3_bucket = s3_bucket
        self.execute_date_time = execute_date_time
        self.config = config
        self.interval_days = interval_days
        self.rdbms_etl_factory = RdbmsEtlFactory(config.db_uri,
                                                config.db_user,
                                                config.db_password)
        self.process_date = DatetimeFactory(execute_date_time, CommonConstant.KST, CommonConstant.KST_START_HHmmss, CommonConstant.KST_END_HHmmss, CommonConstant.ZERO).interval(interval_days)
        self.target_database = f'{config.database_prefix}glue_database'
        self.target_path = f"{self.config.storage_uri}{self.s3_bucket}/{StoragePath.EXAMPLE}"

    def __process(self,
                  table_name: str,
                  where_column: str,
                  query: str ):
            df = self.rdbms_etl_factory.read_rdbms(self.spark, query)
            self.__process_with_df(table_name, where_column, df )

    def __process_with_df(self,
                          table_name: str,
                          where_column: str,
                          df: DataFrame = None) -> None:
            target_path = f"{self.target_path}/{table_name}"
            (df.write.partitionBy('year', 'month', f'{where_column}_date')
                     .format("parquet")
                     .mode(SparkEnv.OVERWRITE)
                     .options(**SparkHelper().overwrite_option_with_parquet_dynamic_partition(SparkEnv.COMPRESSION_GZIP))
                     .option("path", target_path)
                     .saveAsTable(f'{self.target_database}.{table_name}'))


    def daily_batch_05h(self) -> None:
        self.process_date.logging(self.spark)
        self.__process('ald_adj_br_share', 'load_date', Queries().ald_adj_br_share.format(self.process_date.end_date))
        __process_date = DatetimeFactory(self.execute_date_time, CommonConstant.KST, CommonConstant.KST_START_HHmmss, CommonConstant.KST_END_HHmmss).interval(CommonConstant.DEFAULT_ADJUST_DAYS)
        self.__process('store_charge_history', 'action_at', Queries().store_charge_history.format(__process_date.start_date_time, __process_date.end_date_time, CommonConstant.DEFAULT_NLS_DATE_FORMAT))

    def daily_batch_10h(self) -> None:
        self.process_date.logging(self.spark)
        self.__process('system_base_code', 'adjustment_day', Queries().system_base_code.format(self.process_date.end_date))

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=str, required=True)
    parser.add_argument("--s3_bucket", type=str, required=True)
    parser.add_argument("--execute_date_time", type=str, required=True)
    parser.add_argument("--interval_days", type=str, required=False, default=CommonConstant.ONE)
    parser.add_argument("--function_type", type=str, required=False, default='daily_batch_05h')

    env_args = parser.parse_args()
    spark = SparkHelper().create_spark_session()
    config = Config(env_args.env).oracle_info()

    increment = Increment(spark,
                          env_args.s3_bucket,
                          env_args.execute_date_time,
                          config,
                          env_args.interval_days
                          )

    functions = {'daily_batch_05h': increment.daily_batch_05h,
                 'daily_batch_10h': increment.daily_batch_10h,
                 }
    func = functions[env_args.function_type]
    func()

    spark.stop()
