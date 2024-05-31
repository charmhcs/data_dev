import json
from xmlrpc.client import boolean
from numpy import array
from airflow.models import Variable
from airflow.hooks.base import BaseHook


class EmrUtil(object):

    def __init__(self,
                 emr_name: str,
                 step_concurrency: int = 1,
                 cluster_size: array = [1, 1, 1],
                 cluster_type: str = 'm5.xlarge',
                 cluster_applications: str = [{'Name': 'Spark'}, {'Name': 'Hive'}],
                 spark_dynamicAllocation_enable: str = None  # 값 설정시 "false" or "true"
                 ):

        self.emr_name: str = emr_name
        self.cluster_size: array = cluster_size
        self.cluster_type: str = cluster_type
        self.cluster_applications: json = cluster_applications
        self.step_concurrency = step_concurrency

        if step_concurrency > 1:
            self.spark_dynamicAllocation_enable: str = "false"
        else:
            self.spark_dynamicAllocation_enable: str = "true"

        if spark_dynamicAllocation_enable:
            self.spark_dynamicAllocation_enable = spark_dynamicAllocation_enable

        self.env: str = Variable.get("env")
        self.aws_log_uri: str = Variable.get("aws_log_uri")
        self.s3_bucket: str = Variable.get("s3_bucket")
        self.aws_emr_release_label: str = Variable.get("aws_emr_release_label")
        self.aws_emr_ec2_subnet_ids: str = Variable.get("aws_emr_ec2_subnet_ids")
        self.aws_emr_master_security_group: str = Variable.get("aws_emr_master_security_group")
        self.aws_emr_slave_security_group: str = Variable.get("aws_emr_slave_security_group")
        self.aws_emr_service_access_security_group: str = Variable.get("aws_emr_service_access_security_group")
        self.aws_emr_job_flow_role: str = Variable.get("aws_emr_job_flow_role")
        self.aws_emr_service_role: str = Variable.get("aws_emr_service_role")
        self.hive_metastore_connection = BaseHook.get_connection("mysql_conn_hivemetastore")
        self.build_path_spark =  Variable.get("build_path_spark")

    def default_job_flow_override(self,
                                  bootstrap_name: str = 'preinstall'):
        return {
            'Name': f'{self.env}-{self.emr_name}',
            'LogUri': f'{self.aws_log_uri}',
            'ReleaseLabel': f'{self.aws_emr_release_label}',
            'Applications': self.cluster_applications,
            'BootstrapActions': self.bootstrap_action(bootstrap_name),
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': f'{self.cluster_type}',
                        'InstanceCount': self.cluster_size[0],
                    },
                    {
                        'Name': "Core nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': f'{self.cluster_type}',
                        'InstanceCount': self.cluster_size[1],
                        'Configurations': [
                            {
                                'Classification': 'docker-conf',
                                'Configurations': [],
                                'Properties': {
                                    'docker.network.create.options': '--driver=bridge --ip-range=10.10.5.0/24 --subnet=10.10.0.0/16',
                                    'docker.network.create.name': 'emr-docker-bridge'
                                }
                            },
                        ],
                    },
                    {
                        'Name': 'Task node',
                        'Market': 'SPOT',
                        'InstanceRole': 'TASK',
                        'BidPrice': '0.7',
                        'InstanceType': f'{self.cluster_type}',
                        'InstanceCount': self.cluster_size[2],
                        'Configurations': [
                            {
                                'Classification': 'docker-conf',
                                'Configurations': [],
                                'Properties': {
                                    'docker.network.create.options': '--driver=bridge --ip-range=10.10.5.0/24 --subnet=10.10.0.0/16',
                                    'docker.network.create.name': 'emr-docker-bridge'
                                }
                            },
                        ],
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False,
                'Ec2SubnetIds': [f'{self.aws_emr_ec2_subnet_ids}'],
                'EmrManagedMasterSecurityGroup': f'{self.aws_emr_master_security_group}',
                'EmrManagedSlaveSecurityGroup': f'{self.aws_emr_slave_security_group}',
                'ServiceAccessSecurityGroup': f'{self.aws_emr_service_access_security_group}',
            },
            'Configurations': self.configrations(),
            'VisibleToAllUsers': True,
            'JobFlowRole': f'{self.aws_emr_job_flow_role}',
            'ServiceRole': f'{self.aws_emr_service_role}',
            'StepConcurrencyLevel': self.step_concurrency,
            'Tags':[
                {
                    'Key':'Team',
                    'Value':'data-develop'
                },
                {
                    'Key':'Service',
                    'Value':'emr'
                }
            ]
        }

    def test_job_flow_override(self, bootstrap_name: str = 'preinstall'):
        return {
            'Name': f'{self.env}-{self.emr_name}',
            'LogUri': f'{self.aws_log_uri}',
            'ReleaseLabel': f'{self.aws_emr_release_label}',
            'Applications': [{'Name': 'Spark'}, {'Name': 'Hive'}, {'Name': 'Zeppelin'}, {'Name': 'Presto'}, {'Name': 'JupyterEnterpriseGateway'}],
            'BootstrapActions': self.bootstrap_action(bootstrap_name),
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': f'{self.cluster_type}',
                        'InstanceCount': self.cluster_size[0],
                    },
                    {
                        'Name': "Core nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': f'{self.cluster_type}',
                        'InstanceCount': self.cluster_size[1],
                        'Configurations': [
                            {
                                'Classification': 'docker-conf',
                                'Configurations': [],
                                'Properties': {
                                    'docker.network.create.options': '--driver=bridge --ip-range=10.10.5.0/24 --subnet=10.10.0.0/16',
                                    'docker.network.create.name': 'emr-docker-bridge'
                                }
                            },
                        ],
                    },
                    {
                        'Name': 'Task node',
                        'Market': 'SPOT',
                        'InstanceRole': 'TASK',
                        'BidPrice': '0.7',
                        'InstanceType': f'{self.cluster_type}',
                        'InstanceCount': self.cluster_size[2],
                        'Configurations': [
                            {
                                'Classification': 'docker-conf',
                                'Configurations': [],
                                'Properties': {
                                    'docker.network.create.options': '--driver=bridge --ip-range=10.10.5.0/24 --subnet=10.10.0.0/16',
                                    'docker.network.create.name': 'emr-docker-bridge-test'
                                }
                            },
                        ],
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetIds': [f'{self.aws_emr_ec2_subnet_ids}'],
                'EmrManagedMasterSecurityGroup': f'{self.aws_emr_master_security_group}',
                'EmrManagedSlaveSecurityGroup': f'{self.aws_emr_slave_security_group}',
                'ServiceAccessSecurityGroup': f'{self.aws_emr_service_access_security_group}',
            },
            'Configurations': self.configrations(),
            'VisibleToAllUsers': True,
            'JobFlowRole': f'{self.aws_emr_job_flow_role}',
            'ServiceRole': f'{self.aws_emr_service_role}',
            'Tags':[
                {
                    'Key':'Team',
                    'Value':'data-develop'
                },
                {
                    'Key':'Service',
                    'Value':'emr'
                }
            ]
        }

    def bootstrap_action(self,
                         bootstrap_name: str):
        if bootstrap_name.__eq__('preinstall'):
            return [
                {
                    'Name': "preinstall",
                    'ScriptBootstrapAction': {
                        'Path': f's3://{self.s3_bucket}{self.build_path_spark}/src/setup/emr/preinstall.sh',
                        'Args': [f'{self.s3_bucket}', f'{self.build_path_spark}']
                    }
                }
            ]
        else:
            return [
                {
                    'Name': "none_preinstall",
                    'ScriptBootstrapAction': {
                    }
                }
            ]

    def configrations(self):
        ENV_LOCAL = 'local'
        if self.env.__eq__(ENV_LOCAL):
            return [
                {
                    "Classification": "hive-site",
                    "Properties": {
                        "javax.jdo.option.ConnectionUserName": f"{self.hive_metastore_connection.login}",
                        "javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver",
                        "javax.jdo.option.ConnectionPassword": f"{self.hive_metastore_connection.password}",
                        "javax.jdo.option.ConnectionURL": f"jdbc:mysql://{self.hive_metastore_connection.host}:{self.hive_metastore_connection.port}/{self.hive_metastore_connection.schema}?createDatabaseIfNotExist=true&charset=utf8"
                    }
                },
                {
                    "Classification": "spark-defaults",
                    "Properties": {
                        "spark.dynamicAllocation.enabled": f"{self.spark_dynamicAllocation_enable}",
                        "spark.executor.extraJavaOptions": "-Dfile.encoding=UTF-8",
                        "spark.driver.extraJavaOptions": "-Dfile.encoding=UTF-8",
                        "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
                        "spark.hadoop.fs.s3a.path.style.access": "false",
                    }
                }
            ]

        else:
            return [
                {
                    "Classification": "hive-site",
                    "Properties": {
                        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                        "hive.metastore.schema.verification": "false"
                    }
                },
                {
                    "Classification": "spark-hive-site",
                    "Properties": {
                        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                        "hive.metastore.schema.verification": "false"
                    }
                },
                {
                    "Classification": "spark-defaults",
                    "Properties": {
                        "spark.dynamicAllocation.enabled": f"{self.spark_dynamicAllocation_enable}",
                    }
                }
            ]
