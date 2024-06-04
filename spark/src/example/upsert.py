from helpers.constant.common import CommonConstant
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from example.queries import Queries
from helpers.constant import StoragePath
from helpers.factory import SparkHelper
from helpers.factory import RdbmsEtlFactory
from helpers.factory import DatetimeFactory
from config import Config
from pyspark.sql.functions import expr



import argparse

class Upsert(object):

    def __init__(self,
                 spark: SparkSession,
                 s3_bucket: str,
                 execute_date_time: str,
                 config: Config):
        self.spark = spark
        self.s3_bucket = s3_bucket
        self.execute_date_time = execute_date_time
        self.config = config
        self.rdbms_etl_factory = RdbmsEtlFactory(config.db_uri,
                                                 config.db_user,
                                                 config.db_password)

        self.target_database = f'{config.database_prefix}glue_database'
        self.target_path = f"{self.config.storage_uri}{self.s3_bucket}/{StoragePath.EXAMPLE}"

    def __process_with_df(self,
                          table_name: str,
                          record_key: str,
                          precombine_key: str,
                          partitioned_key: str,
                          df: DataFrame = None) -> None:
            hudi_options = SparkHelper().write_option_hudi_with_partition(self.config, self.target_database, table_name, record_key, partitioned_key, precombine_key)
            df.write.format("hudi").options(**hudi_options).mode("append").save(f"{self.target_path}/{table_name}")

    def __process(self,
                  table_name: str,
                  record_key: str,
                  precombine_key: str,
                  partitioned_key: str,
                  query: str) -> None:
            df = self.rdbms_etl_factory.read_rdbms(self.spark, query)
            self.__process_with_df(table_name, record_key, precombine_key, partitioned_key, df)

    def daily_batch_05h(self) -> None:
        self.process_date = DatetimeFactory(self.execute_date_time, CommonConstant.KST, CommonConstant.KST_START_HHmmss, CommonConstant.KST_END_HHmmss, CommonConstant.ZERO).interval(CommonConstant.ONE)
        self.process_date.logging(self.spark)
        query_order_record = Queries().order_record.format(self.process_date.start_date_time, self.process_date.end_date_time, CommonConstant.DEFAULT_NLS_DATE_FORMAT, self.config.crypt_tag)
        df_order_record = self.rdbms_etl_factory.read_rdbms(self.spark, query_order_record)
        df_order_record = (
                                df_order_record.withColumn("order_phone_number", expr(f"base64(aes_encrypt(order_phone_number, '{self.config.crypt_key}', 'GCM'))"))
                                                .withColumn("order_phone_number2", expr(f"base64(aes_encrypt(order_phone_number2, '{self.config.crypt_key}', 'GCM'))"))
                            )
        self.__process_with_df('order_record', 'order_id', 'created_at', 'year, month, order_date, create_date', df_order_record)


    def daily_batch_10h(self) -> None:
        self.process_date = DatetimeFactory(self.execute_date_time, CommonConstant.KST, CommonConstant.KST_START_HHmmss, CommonConstant.KST_END_HHmmss).default()
        self.process_date.logging(self.spark)
        self.__process('adjustment_cash', 'sequence_no',  'updated_at', 'year, month, create_date', Queries().adjustment_cash.format(self.process_date.start_date_time, self.process_date.end_date_time, CommonConstant.DEFAULT_NLS_DATE_FORMAT))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=str, required=True)
    parser.add_argument("--s3_bucket", type=str, required=True)
    parser.add_argument("--execute_date_time", type=str, required=True)
    parser.add_argument("--function_type", type=str, required=False, default='daily_batch_05h')

    env_args = parser.parse_args()
    spark = SparkHelper().create_spark_session()
    config = Config(env_args.env).oracle_info()

    upsert = Upsert(spark,
                    env_args.s3_bucket,
                    env_args.execute_date_time,
                    config)

    functions = {'daily_batch_05h': upsert.daily_batch_05h,
                 'daily_batch_10h': upsert.daily_batch_10h,
                 }
    func = functions[env_args.function_type]
    func()

    spark.stop()