from pyspark.sql import SparkSession
from helpers.factory.datetime_factory import DatetimeFactory
from helpers.constant.sparkenv import SparkEnv
from helpers.constant.common import CommonConstant
from helpers.factory.spark_helper import SparkHelper


class RdbmsEtlFactory(object):
    def __init__(self,
                 jdbc_url: str,
                 user: str,
                 password: str,
                 num_partitions: int = SparkEnv.NUM_PARTITIONS_SMALL,
                 jdbc_driver: str = SparkEnv.JDBC_DRIVER_DEFAULT,
                 fetch_size: int = SparkEnv.FETCH_SIZE_DEFAULT):
        self.jdbc_url = jdbc_url
        self.user = user
        self.password = password
        self.num_partitions = num_partitions
        self.jdbc_driver = jdbc_driver
        self.fetch_size = fetch_size

    def read_rdbms(self, spark: SparkSession, query: str):
        log = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        log.info(f"query : {query}")
        dbtable = f"({query}) TEMP_DBTABLE"

        jdbc_df = (spark.read
                        .format("jdbc")
                        .option("driver", self.jdbc_driver)
                        .option("url", self.jdbc_url)
                        .option("user", self.user)
                        .option("password", self.password)
                        .option("dbtable", dbtable)
                        .option("fetchsize", self.fetch_size)
                        .load())
        return jdbc_df.toDF(*[c.lower() for c in jdbc_df.columns])
        # .option("sessionInitStatement", "ALTER SESSION SET TIME_ZONE = '+00:00'")

    def read_rdbms_with_partition(self,
                                  spark: SparkSession,
                                  process_date: DatetimeFactory,
                                  query: str,
                                  partition_key: str,
                                  timestampFormat: str = CommonConstant.DEFAULT_NLS_DATE_FORMAT,
                                  sessionInitStatement: str = f"ALTER SESSION SET NLS_DATE_FORMAT = '{CommonConstant.DEFAULT_NLS_DATE_FORMAT}'"):
        log = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        log.info(f"query : {query}")
        dbtable = f"({query}) TEMP_DBTABLE"
        jdbc_df = (spark.read
                        .format("jdbc")
                        .option("driver", self.jdbc_driver)
                        .option("url", self.jdbc_url)
                        .option("user", self.user)
                        .option("password", self.password)
                        .option("dbtable", dbtable)
                        .option("fetchsize", self.fetch_size)
                        .option("numPartitions", self.num_partitions)
                        .option("partitionColumn", partition_key)
                        .option("lowerBound", process_date.start_date)
                        .option("upperBound", process_date.end_date)
                        .option("timestampFormat", timestampFormat)
                        .option("sessionInitStatement", sessionInitStatement)
                        .load())
        return jdbc_df.toDF(*[c.lower() for c in jdbc_df.columns])

    def rdbms_to_s3_copy(self,
                         spark: SparkSession,
                         table_name: str,
                         target_path: str,
                         compression: str = SparkEnv.COMPRESSION_DEFAULT):

        query = f"""
                (SELECT  A.* FROM   {table_name} A) TEMP_DBTABLE
            """
        log = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)
        log.info(f"target_path : {target_path}")
        log.info(f"query : {query}")

        jdbc_df = (spark.read
                        .format("jdbc")
                        .option("driver", self.jdbc_driver)
                        .option("url", self.jdbc_url)
                        .option("user", self.user)
                        .option("password", self.password)
                        .option("dbtable", query)
                        .option("fetchsize", self.fetch_size)
                        .load())
        df = jdbc_df.toDF(*[c.lower() for c in jdbc_df.columns])
        (df.coalesce(SparkEnv.NUM_PARTITIONS_ONE)
           .write
           .format("parquet")
           .mode(SparkEnv.OVERWRITE)
           .options(**SparkHelper().write_option_with_parquet(compression))
           .save(target_path))
