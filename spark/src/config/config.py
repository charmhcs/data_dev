from helpers.constant.common import CommonConstant


class Config():
    def __init__(self,
                 env: str = CommonConstant.ENV_TEST):
        self.env = env
        self.hudi_hive_sync = "true"
        self.database_prefix = ''
        self.storage_uri = "s3://"
        self.aws_s3_endpoint_url: str = "http://localhost:9000"
        self.aws_s3_access_key: str = "minio"
        self.aws_s3_secret_key: str = "miniopass"


    def default(self):
        return self
