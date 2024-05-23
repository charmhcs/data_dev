from airflow.models.baseoperator import BaseOperator

import awswrangler as wr
import logging
import pandas as pd


class AthenaToS3Operator(BaseOperator):
    """
        Atehan query 결과값을 s3에 upload 함
    """
    template_fields = ('athena_query', 'target_s3_path')
    template_ext = ('.sql')
    template_fields_renderers = {"athena_query": "sql"}

    def __init__(self,
                 athena_query: str,
                 target_s3_path: str,
                 athena_database: str = 'default',
                 save_format: str = 'parquet',
                 compression: str = 'gzip',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.athena_query = athena_query
        self.athena_database = athena_database
        self.target_s3_path = target_s3_path
        self.save_format = save_format
        self.compression = compression

    def df_to_s3(self, df, target_s3_path, save_format, compression):
        logging.info(f'target_s3_path : {target_s3_path}')
        logging.info(f'save_format : {save_format}')
        logging.info(f'compression : {compression}')

        if df is None:
            raise ValueError('df must not be None')

        if save_format not in ['parquet', 'csv', 'json']:
            raise ValueError("save_format must be in ['parquet', 'csv', 'json']")

        if compression not in [None, 'gzip', 'snappy']:
            raise ValueError("compression must be in [None, 'gzip', 'snappy']")

        if save_format == 'parquet':
            wr.s3.to_parquet(df=df, path=target_s3_path, compression=compression, index=False)
        elif save_format == 'csv':
            wr.s3.to_csv(df=df, path=target_s3_path, compression=compression, index=False, header=False)
        elif save_format == 'json':
            wr.s3.to_json(df=df, path=target_s3_path, compression=compression, index=False)

    def execute(self, context):
        logging.info(self.athena_query)
        df = wr.athena.read_sql_query(
            sql=self.athena_query,
            database=self.athena_database
        )
        logging.info(df.head())

        self.df_to_s3(df,
                      target_s3_path=self.target_s3_path,
                      save_format=self.save_format,
                      compression=self.compression)