from helpers.constant.sparkenv import SparkEnv
from pyspark.sql import SparkSession
from helpers.constant import StoragePath
from helpers.factory import SparkHelper
from helpers.factory import RdbmsEtlFactory
import argparse
from example.queries import Queries
from config import Config


class FullCopy():
    def batch_daily(self,
                    spark: SparkSession,
                    s3_bucket: str,
                    config: Config
                    ):

        rdbms_etl_factory = RdbmsEtlFactory(config.db_uri,
                                            config.db_user,
                                            config.db_password)

        log = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        log.info(f"s3_bucket : {s3_bucket}")

        rdbms_etl_factory.rdbms_to_s3_copy(spark, f'data.adjustment_brand', f'{config.storage_uri}{s3_bucket}/{StoragePath.EXAMPLE}/adjustment_brand', SparkEnv.COMPRESSION_GZIP)
        SparkHelper().write_parquet_object(rdbms_etl_factory.read_rdbms(spark, Queries().worker_info), f'{config.storage_uri}{s3_bucket}/{StoragePath.EXAMPLE}/worker_info', SparkEnv.COMPRESSION_GZIP)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", type=str, required=True)
    parser.add_argument("--s3_bucket", type=str, required=True)

    env_args = parser.parse_args()
    config = Config(env_args.env).oracle_info()
    spark = SparkHelper().create_spark_session()
    FullCopy().batch_daily(spark, env_args.s3_bucket, config)
    spark.stop()
