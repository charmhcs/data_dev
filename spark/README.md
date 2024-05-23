
# AWS EMR로 데이터 분석을 위한 pySpark 개발환경 구축 환경 설정 for intel macbook

## 1. 정의 
- python을 사용하여 Apache Spark 개발 환경을 구축
- 가상환경 및 IDE를 사용하여 Apache Spark 동작 구현 및 단위 테스트 환경
- docker를 사용하여 airflow + EMR 사용을 통한 오케스트레이션 및 통합 테스트 환경
- AWS EMR의 Apache Spark 환경에 다른 종속성이 없도록  패키징 및 배포 환경 구축
- naming rule, common module등 기준을 만들어서 코드의 일관성을 유지하도록 함

## 2. 설치 방법 

- [Conda를 활용한 개발환경 구축](https://www.notion.so/barogohq/miniConda-4dd38c45f3794bd1b34051fa4bcc01c6)



# 변경사항 이력 

## 2021-12-27 
EMR6을 적용하기 위해, [Conda를 활용한 개발환경 구축] 문서 업데이트 완료 (2021.12.27) 
- requirements.txt 를  requirements.txt 및 requirements-tools.txt로 분리함
- requirements-tools.txt는 conda 환경의 개발 도구 모듈 목록을 저장
- requirements.txt는 pySpark 동작 환경에 필요한 외부 모듈 목록을 저장

pyTest를 위해 AWS S3을 접근하기 위해, hadoop-aws 대신, hadoop-cloud-storage로 라이브러리를 변경함
transient EMR 동작 및 배포 자동화를 위한 build.sh 초안 작성 