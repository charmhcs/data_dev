from config.config import Config
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from helpers.constant.sparkenv import SparkEnv
from pyspark import SparkConf

class SparkHelper:

    def __init__(self):
        pass

    def create_spark_session(self):
        conf = SparkConf()
        # 데이터에서 시간 포맷이 다를 경우 현재 설정으로 바로 잡음
        # conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        # 파티션 사이즈 128MB로 설정
        # conf.set("spark.sql.files.maxpartitionBytes", "134217728")
        # spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate() <- 이런 내용들은 spark-submit시 적용
        return SparkSession.builder.enableHiveSupport().getOrCreate()

    def write_parquet_object(self,
                             df: DataFrame,
                             target_path: str,
                             compression_type: str = SparkEnv.COMPRESSION_DEFAULT):
        (df.write
           .format("parquet")
           .mode(SparkEnv.OVERWRITE)
           .options(**self.write_option_with_parquet(compression_type))
           .save(target_path))

    def write_table_parquet_dynamic_partition(df: DataFrame,
                                              base_target_path_name: str,
                                              target_table_name: str,
                                              target_database_name : str,
                                              partition_column_List : list,
                                              compression_type: str = SparkEnv.COMPRESSION_DEFAULT
                                              ):
        (df.write.partitionBy(partition_column_List)
            .format("parquet")
            .mode(SparkEnv.OVERWRITE)
            .options(**SparkHelper().overwrite_option_with_parquet_dynamic_partition(compression_type))
            .option("path", f"{base_target_path_name}/{target_table_name}")
            .saveAsTable(f"{target_database_name}{target_table_name}"))

    def write_option_with_parquet(self,
                                  compression_type: str = SparkEnv.COMPRESSION_DEFAULT):
        write_option = {
            "compression": compression_type,
            'parquet.enable.dictionary': 'true',
            'parquet.block.size': SparkEnv.PARQUET_BLOCK_SIZE_DEFAULT,
            'parquet.page.size': SparkEnv.PARQUET_PAGE_SIZE_DEFAULT,
            'parquet.dictionary.page.size': SparkEnv.PARQUET_DICTIONARY_PAGE_SIZE_DEFAULT,
        }
        return write_option

    def overwrite_option_with_parquet_dynamic_partition(self,
                                                        compression_type: str = SparkEnv.COMPRESSION_DEFAULT):
        write_option = {
            "compression": compression_type,
            'partitionOverwriteMode': 'dynamic',
            'parquet.enable.dictionary': 'true',
            'parquet.block.size': SparkEnv.PARQUET_BLOCK_SIZE_DEFAULT,
            'parquet.page.size': SparkEnv.PARQUET_PAGE_SIZE_DEFAULT,
            'parquet.dictionary.page.size': SparkEnv.PARQUET_DICTIONARY_PAGE_SIZE_DEFAULT,
        }
        return write_option

    def write_option_hudi_without_partition(self,
                                            target_database: str,
                                            target_table : str,
                                            record_key : str,
                                            precombine_key : str,
                                            operation : str = 'upsert'
                                            ):
        hudi_options = {
            'hoodie.database.name': target_database,
            'hoodie.table.name': target_table,
            'hoodie.datasource.write.recordkey.field': record_key,
            'hoodie.datasource.write.table.name': target_table,
            'hoodie.datasource.write.operation': operation,
            'hoodie.datasource.write.precombine.field': precombine_key,
            'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
            'hoodie.cleaner.fileversions.retained': 3,
        }
        return hudi_options

    def write_option_hudi_with_partition(self,
                                    config : Config,
                                    target_database: str,
                                    target_table : str,
                                    record_key : str,
                                    partition_key : str,
                                    precombine_key : str,
                                    operation : str = 'upsert'
                                    ):
        hudi_options = {
            'hoodie.database.name': target_database,
            'hoodie.table.name': target_table,
            'hoodie.datasource.write.recordkey.field': record_key,
            'hoodie.datasource.write.partitionpath.field': partition_key,
            'hoodie.datasource.write.table.name': target_table,
            'hoodie.datasource.write.operation': operation,
            'hoodie.datasource.write.precombine.field': precombine_key,
            'hoodie.datasource.write.hive_style_partitioning': 'true',
            'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator',
            'hoodie.datasource.hive_sync.enable': config.hudi_hive_sync,
            'hoodie.datasource.hive_sync.database': target_database,
            'hoodie.datasource.hive_sync.table': target_table,
            'hoodie.datasource.hive_sync.partition_fields': partition_key,
            'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
            'hoodie.datasource.hive_sync.support_timestamp': 'true',
            'hoodie.parquet.compression.codec': 'gzip',
            'hoodie.cleaner.policy': 'KEEP_LATEST_FILE_VERSIONS',
            'hoodie.cleaner.fileversions.retained': 3,
        }
        return hudi_options

