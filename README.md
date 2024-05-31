# 1 데이터 분석을 위한 환경 구축

### 1.1 로컬 PC 환경 설정

1. [Conda 설치 및 Apache Spark with AWS EMR (pySpark) &  AWS S3 (loacl minio)](https://github.com/charmhcs/data_dev/tree/master/spark#readme)
2. [DTB with Athena](https://github.com/charmhcs/data_dev/tree/master/dbt)
3. Airflow (작성중)

# 2 AWS EMR를 이용한 분석환경 배포 및 실행

### 2.1 spark 배포 워크플로
![](https://github.com/charmhcs/data_dev/blob/master/pyspark.drawio.png?raw=true)

### 2.2 배포

```bash
(etl)% ./spark/build.sh {s3_bucket_name} {build_path}
```
- {s3_bucket_name} s3://는 제외한 나머지 s3 버킷명을 표기함
  - 예로 s3://data_dev인 경우  {s3_bucket_name}는 data_dev 가 됨
- {build_path} 는 여러 사용자가 동시에 bucket를 공용으로 사용하는 경우, 각자의 build_path를 유니크하게 지정하여 pyspark 파일 및 whl 배포파일을 업로드 함
  - charmhcs로 build path를 설정하는 경우 {build_path}는 /charmhcs 로 지정함
  - 앞에 /를 붙이는 이유는 객채의 depth를 사용자가 조정하기 위해서임
  ```bash
      # /spark/etc/setup/emr/preinstall.sh
      #!/bin/bash -xe
      s3_bucket=${1}
      build_path=${2}
      sudo aws s3 cp s3://${s3_bucket}${build_path}/src/setup/etl_common-0.1-py3-none-any.whl ./
      sudo python3 -m pip install etl_common-0.1-py3-none-any.whl
  ```