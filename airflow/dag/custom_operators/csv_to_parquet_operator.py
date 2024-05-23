import pandas as pd
import io
import logging
from decimal import Decimal
import re

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

log = logging.getLogger(__name__)


class CsvToParquetOperator(BaseOperator):
    template_fields = ('bucket_name', 'source_s3_key', 'dest_s3_key')

    def __init__(
        self,
        bucket_name: str,
        source_s3_key: str,
        dest_s3_key: str,
        columns,
        dtypes=None,
        is_single_file=True,
        aws_conn_id: str = 'aws_default',
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.source_s3_key = source_s3_key
        self.dest_s3_key = dest_s3_key
        self.columns = columns
        self.dtypes = dtypes
        self.is_single_file = is_single_file
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        log.info(f'bucket_name : {self.bucket_name}')
        log.info(f'source_s3_key : {self.source_s3_key}')
        log.info(f'dest_s3_key : {self.dest_s3_key}')
        log.info(f'columns : {self.columns}')
        log.info(f'is_single_file : {self.is_single_file}')

        s3_hook = S3Hook(aws_conn_id='aws_default')

        if not s3_hook.check_for_key(self.source_s3_key, bucket_name=self.bucket_name):
            log.info(f'No object in {self.source_s3_key}')
            return
        
        csv_files = []
        if self.is_single_file:
            csv_files.append(s3_hook.get_key(self.source_s3_key, bucket_name=self.bucket_name).get()['Body'])
        else:
            prefix = '_'.join(self.source_s3_key.split('_')[:-1]) # t0/thirdparty/ms/excel/ms_ref_m_2023-06-08.csv에서 2023-06-08.csv 이전부분 추출
            key_list = s3_hook.list_keys(bucket_name=self.bucket_name, prefix=prefix)
            for key in key_list:
                if key[-4:] == '.csv':
                    csv = s3_hook.get_key(key, bucket_name=self.bucket_name).get()['Body']
                    csv_files.append(csv)
        parquet = self.csv_to_parquet(csv_files)

        s3_hook.load_bytes(
            parquet,
            self.dest_s3_key,
            bucket_name=self.bucket_name,
            replace=True,
        )

        log.info(f'Successfully converted {self.source_s3_key} to {self.dest_s3_key}')

    def csv_to_parquet(self, csv_files):
        dfs = []
        for csv_file in csv_files:
            dfs.append(pd.read_csv(csv_file, header=None, names=self.columns))
        df = pd.concat(dfs)
        log.info(f'df shape {df.shape}')

        if self.dtypes is not None:
            for column, dtype in zip(df.columns, self.dtypes):
                if dtype.startswith('decimal'):
                    df = self.df_astype_decimal(df, column, dtype)
                    continue
                df[column] = df[column].astype(dtype)
        log.info(f'df dtypes {df.dtypes}')

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', compression='gzip', index=False)

        return parquet_buffer.getvalue()

    def df_astype_decimal(self, df, column, dtype):
        pattern = r"\d+"
        matches = re.findall(pattern, dtype)
        precision, scale = [int(match) for match in matches]
        
        df[column] = df[column].apply(lambda x: Decimal(x).quantize(Decimal('0' * (precision - scale) + '.' + '0' * scale)))
        return df
