# 데이터 분석을 위한 환경 구축

### 1. 환경 설정 

- [Conda 설치 및 Apache Spark with AWS EMR (pySpark) &  AWS S3 (loacl minio)](https://github.com/charmhcs/data_dev/tree/master/spark#readme)
- [DTB with Athena](https://github.com/charmhcs/data_dev/tree/master/dbt)

### 2. spark 배포 워크플로 
![](https://github.com/charmhcs/data_dev/blob/master/pyspark.drawio.png?raw=true)

### 3. 배포 

```bash
(etl)% /spark/build.sh {s3_bucket} {build_path}
```
