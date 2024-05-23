import boto3
from typing import List

from config import Config
from helpers.constant.common import CommonConstant


class ObjectStorageFactory(object):
    def __init__(self,
                 config: Config):
        self.config = config
        self._s3_init()

    def _s3_init(self):
        if self.config.env == CommonConstant.ENV_TEST:
            s3_resource = boto3.resource(service_name='s3',
                                         endpoint_url=self.config.aws_s3_endpoint_url,
                                         aws_access_key_id=self.config.aws_s3_access_key,
                                         aws_secret_access_key=self.config.aws_s3_secret_key)
        else:
            s3_resource = boto3.resource(service_name='s3')
        self.s3_resource = s3_resource

    def _get_s3_bucket(self, bucket_name: str):
        return self.s3_resource.Bucket(bucket_name)

    def delete_all_s3_object(self,
                             bucket_name: str,
                             prefix: str) -> None:
        bucket = self._get_s3_bucket(bucket_name)

        for obj in bucket.objects.filter(Prefix=prefix):
            self.s3_resource.Object(bucket.name, obj.key).delete()

    def check_object_exists(self,
                            bucket_name: str,
                            prefix: str) -> bool:
        """
        prefix에 object가 하나라도 존재하는지 확인
        """
        bucket = self._get_s3_bucket(bucket_name)
        obj = bucket.objects.filter(Prefix=prefix).limit(1)
        return len(list(obj)) > 0

    def get_object_list(self,
                        bucket_name: str,
                        prefix: str) -> List:
        """
        prefix에 해당하는 모든 object path를 리스트로 반환
        """
        bucket = self._get_s3_bucket(bucket_name)

        object_paths = []
        for obj in bucket.objects.filter(Prefix=prefix):
            object_paths.append(obj.key)

        return object_paths