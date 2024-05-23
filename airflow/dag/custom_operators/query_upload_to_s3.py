from typing import Sequence
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class QueryUploadToS3(BaseOperator):
    template_fields: Sequence[str] = ('query', 'bucket')
    template_ext: Sequence[str] = ('.sql', '.hql')
    template_fields_renderers = {"query": "sql"}

    def __init__(self,
                 query: str,
                 bucket: str,
                 location: str,
                 aws_conn_id='aws_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        self.bucket = bucket
        self.location = location
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        s3_hook = S3Hook(self.aws_conn_id)
        s3_hook.load_string(self.query, self.location, bucket_name=self.bucket, replace=True)
