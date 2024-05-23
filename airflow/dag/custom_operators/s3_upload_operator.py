import os
import pandas as pd
import awswrangler as wr

from pprint import pformat
from pathlib import Path

from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3UploadOperator(BaseOperator):

    template_fields = ('data',
                       'data_path',
                       'target_path',
                       'write_option')

    def __init__(self,
                 target_path,
                 data: any = None,
                 data_path: str = None,
                 source_extension: str = None,
                 target_extension: str = 'parquet',
                 read_option: dict = {},
                 write_option: dict = {},
                 *args,
                 **kwargs):
        """
        data 나 data_path 로 source data를 넣어줌
        data:
            - dict /json 형태의 데이터
        data_path:
            - file path
            - s3 url or 코드의 include dir 안의 file path  
                - ex. s3://local.barogo-data/your/source/path
                - ex. (코드의 include 폴더 안의) t0_database/json/gorela/gorela_table_category.json
        target_path:
            - 데이터를 저장할 s3 위치
        source_extension:
            - 데이터 형식. 사용자가 지정 안 하면 infer해서 적용. default=None
        target_extension:
            - 데이터를 저장할 때 파일 extenstion 지정. default=parquet
        read_option:
            - pd.read_table(), pd.read_csv(), pd.read_json() .. 등의 pandas read_* 함수의 params
        write_option:
            - wr.s3.to_parquet , wr.s3.to_parquet 등의 awswrangler 각 to_* 함수의 params
        """

        super().__init__(*args, **kwargs)
        self.data = data
        self.data_path = data_path
        self.source_extension = source_extension
        self.target_path = target_path
        self.read_option = read_option
        self.write_option = write_option
        self.target_extension = target_extension
        self.read_func = {
            "csv": pd.read_csv,
            "tsv": pd.read_table,
            "json": pd.read_json,
            "dict": pd.DataFrame.from_dict
        }

    def execute(self, context):
        """데이터프레임을 s3에 저장
        - awsrangler 가 데이터프레임을 파티션처리하여 s3로 저장함
        - task를 재실행할 때 기존 데이터가 있다면 기본 overwrite
        """

        # data / data_path 검사
        self._validate()

        if self.data:
            self.log.info(f"data: {self.data}")
            df = self.get_df_from_data()
        elif self.data_path:
            self.log.info(f"data path: {self.data_path}")
            df = self.get_df_from_path()
        self.log.info(f"target_path: {self.target_path}")

        # json column datatype 이 숫자일 경우
        # IndexError: index out of bounds 에러나서 컬럼값 str 로 처리
        df.columns = df.columns.astype('str')
        self.log.info("df: ")
        self.log.info(df.info())
        self.log.info(pformat(df))
        self.upload_df(df)

    def _validate(self):
        """source data / data_path 값 검사"""
        # data 나 data 경로 둘다 None인 경우
        if not self.data and not self.data_path:
            self.log.error(f"Data and data_path both are None.")
            raise AirflowException()

        # data 나 data 경로 둘다 값이 있는 경우
        elif self.data and self.data_path:
            self.log.error(f"""Data and data_path both exist.
            - data_path: {self.data_path}
            - data: {self.data}""")
            raise AirflowException()

    def get_df_from_data(self):
        """dict나 json 데이터로 df 생성."""

        self.log.info(type(self.data))

        # dict 형태인 경우 ******
        if isinstance(self.data, dict):
            type_ = self.get_data_type(type='dict')
            return self.read_func[type_](self.data, **self.read_option)

        # str 형태일 경우 *******
        elif isinstance(self.data, str):
            # 1. json ******************
            if self.is_json():
                type_ = self.get_data_type(type='json')
                return self.read_func[type_](self.data, **self.read_option)

            # 4. etc *******************
            else:
                raise Exception(f"can't find a proper function to convert data to dataframe: {self.data}")

    def get_df_from_path(self):
        """data 경로로 df 생성"""
        type_ = self.get_data_type(type=Path(self.data_path).suffix.replace('.', ''))

        # 1. s3 파일 ****************
        if self.is_s3_path():
            s3_hook = S3Hook(aws_conn_id='aws_default')
            data = s3_hook.get_key(self.data_path).get()['Body']
            return self.read_func[type_](data, **self.read_option)

        # 2. include dir 파일 *******
        # data path 로 들어와 파일 extension이 있는 경우 (.json/.csv/.tsv)
        elif self.is_in_include_dir():
            return self.read_func[type_](self.get_local_path(), **self.read_option)

        # 4. etc *******************
        else:
            raise Exception(f"can't find a proper function to convert data to dataframe: {self.data}")

    def upload_df(self, df):
        """datafrmae 을 s3에 파일로 업로드"""
        upload_func = {
            'parquet': wr.s3.to_parquet,
            'csv': wr.s3.to_csv,
            'excel': wr.s3.to_excel,
            'json': wr.s3.to_json,
        }

        write_option = {
            'dataset': True,
            'compression': 'gzip'  # 'snappy', 'gzip', 'brotli', None 가능
        }

        write_option.update(self.write_option)

        self.log.info(write_option)
        upload_func[self.target_extension](
            df=df,
            path=f"{self.target_path}",
            **write_option
        )

    def is_json(self):
        """json 인지 확인"""
        import json
        try:
            self.log.info(json.loads(self.data))
            return True
        except ValueError:
            return False

    def is_s3_path(self):
        """s3파일인지 확인"""
        return 's3://' in self.data_path and wr.s3.does_object_exist(self.data_path)

    def is_in_include_dir(self):
        """code include dir의 파일인지 확인"""
        return os.path.exists(self.get_local_path())

    def get_data_type(self, type):
        """source data 확장자 리턴. 사용자가 지정한 source 확장자 우선 리턴"""
        return self.source_extension if self.source_extension else type

    def get_local_path(self):
        """"""
        return f"{Variable.get('airflow_base_path')}/include/{self.data_path}"

