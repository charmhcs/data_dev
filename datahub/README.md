# 1. 로컬 환경

## 1.1 DataHub cli 설치

python 가상 환경을 별도로 만들고 생성된 가상 환경에 datahub cli 를 pip 로 설치 하는 형태로 구성함

- datahub cli 설치

```bash
(base)% conda create -n datahub #python verson은 3.10 이상
(base)% conda activate datahub
(datahub)% python3 -m pip install --upgrade pip wheel setuptools
(datahub)% python3 -m pip install --upgrade acryl-datahub
# validate that the install was successful
```

- datahub cli 설치 확인

```bash
(datahub)% datahub version
# If you see "command not found", try running this instead: python3 -m datahub version
DataHub CLI version: 0.10.2.1
Python version: 3.7.4 (v3.7.4:e09359112e, Jul  8 2019, 14:54:52)
[Clang 6.0 (clang-600.0.57)]
```

## 1.2 datahub cli init - gms 서버와 통신 가능하게 설정

- datahub token 생성
    - WEB UI 에서 Access Token 발급
- datahub cli init
    - 정보가 잘못 들어갔을 경우 다시 `datahub init`으로 기존 host, token 정보를 갱신할 수 있음

```bash
(datahub)%  datahub init
/Users/cshwang/.datahubenv already exists. Overwrite? [y/N]: y
Configure which datahub instance to connect to
Enter your DataHub host [http://localhost:8080]: https://dev-api-datahub.io
Enter your DataHub access token (Supports env vars via `{VAR_NAME}` syntax) []:
Written to /Users/cshwang/.datahubenv
```

## 1.3 간단한 datahub cli command

### datahub 에 등록 되어 있는 metadata 조회

```bash
❯ datahub get --urn "urn:li:dataset:(urn:li:dataPlatform:glue,order.t1_db_order_history,PROD)"
{
  "browsePaths": {
    "paths": [
      "/prod"
    ]
  },
  "container": {
    "container": "urn:li:container:ce0c951bbd8caf90880822d6a2da1eb6"
  },
  "dataPlatformInstance": {
    "instance": "urn:li:dataPlatformInstance:(urn:li:dataPlatform:glue,order)",
    "platform": "urn:li:dataPlatform:glue"
  },
...
```

# 3. 주요 cli command

### 3.1 datahub cli 로 metadata 삭제

[Removing Metadata from DataHub | DataHub](https://datahubproject.io/docs/how/delete-metadata/)

특정 metadata 만 metadata urn 으로 삭제

- Soft Delete

    `—-soft` 옵션이 `datahub delete` command 에서 default

    status 값을 `Removed` 로 변경 하여 UI 에서만 안보이게 처리됨

    ```bash
    ❯ datahub delete --urn "urn:li:dataset:(urn:li:dataPlatform:glue,order.t1_db_order_order.alarms,PROD)" --soft
    [2023-04-24 16:58:44,319] INFO     {datahub.cli.delete_cli:182} - DataHub configured with http://dev-api-datahub.io
    Successfully deleted urn:li:dataset:(urn:li:dataPlatform:glue,order.t1_db_order_order.alarms,PROD). -1 rows deleted
    Took 1.881 seconds to soft delete -1 versioned rows and 0 timeseries aspect rows for 1 entities.
    ```

- Hard Delete

    물리적으로 모든 관련된 metadata 를 삭제

    삭제 하려는 metadata 와 관련된 모든 것들을 삭제 해주므로 깨끗하게 지워주긴 하지만 되돌릴 수 없으므로 숙고 한뒤 실행 해야 함

    ```bash
    datahub delete --urn "<my urn>" --hard
    ```

    `—dry-run` 옵션으로 command 로 먼저 확인 해보는게 필요

    `—force` 으로 강제 삭제도 가능

    `-only-soft-deleted` 으로 soft delete 된 것만 삭제 하는 것도 가능

- filter 로 지우기

    PROD 에 속한 모든 dataset 삭제

    ```bash
    (datahub)%  datahub delete --entity_type dataset --env PROD
    [2023-04-25 08:48:43,133] INFO     {datahub.cli.delete_cli:290} - datahub configured with http://dev-api-datahub.io
    [2023-04-25 08:48:43,700] INFO     {datahub.cli.delete_cli:327} - Filter matched 69  dataset entities of None. Sample: ['urn:li:dataset:(urn:li:dataPlatform:glue,order.t1_db_order_stts_appsis.ald_grp_br_change,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:glue,order.t1_db_order_appsis_old.ald_a01_record_old,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:glue,order.t1_db_order_appsis_old.ald_a01_record,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:glue,order.t1_db_order_stts_appsis.ald_adj_hq_cash_log,PROD)', 'urn:li:dataset:(urn:li:dataPlatform:glue,order.t1_db_order_stts_appsis.ald_adj_br_cash_log,PROD)']
    This will soft delete 69 entities. Are you sure? [y/N]: y
    100% (69 of 69) |##########################################################################################################################################################| Elapsed Time: 0:00:03 Time:  0:00:03
    Took 11.656 seconds to soft delete -1 versioned rows and 0 timeseries aspect rows for 69 entities.
    ```

    t0_databsae 모두 삭제

    ```
    # 삭제 하기전에 --dry-run option 으로 삭제 하려는 dataset 반드시 확인
    datahub delete --entity-type dataset --platform glue --env DEV --query "t0_database" --dry-run

    datahub delete --entity-type dataset --platform glue --env DEV --query "t0_database"
    ```


### 3.2 datahub cli 로 삭제 하려는 ingest rollback

이미 가져온 metadata 에 대한 ingest 를 run id 단위로 rollback 하여 특정 ingest run 에서 수집 한 모든 metadata 를 삭제

- datahub ui 에서도 각 run 단위로 rollback 기능 제공
- 삭제 하려는 ingrest runId 확인

    ```bash
    (datahub)%  datahub ingest list-runs
    +--------------------------------------+--------+---------------------------+
    | runId                                |   rows | created at                |
    +======================================+========+===========================+
    | no-run-id-provided                   |    248 | 2023-04-24 16:28:56 (KST) |
    +--------------------------------------+--------+---------------------------+
    | 5d7ae53c-a82f-4a07-94a7-e5b8b6458c7d |    794 | 2023-04-24 00:00:11 (KST) |
    +--------------------------------------+--------+---------------------------+
    | e442c9ae-d7a9-423a-b064-c1e775332f08 |     82 | 2023-04-24 00:00:11 (KST) |
    +--------------------------------------+--------+---------------------------+
    | 44e1069e-7715-4e75-a05f-52297165f767 |    179 | 2023-04-24 00:00:10 (KST) |
    +--------------------------------------+--------+---------------------------+
    | ba3e864d-a22b-4ee0-a93e-ebc4ebc6d487 |      7 | 2023-04-24 00:00:10 (KST) |
    +--------------------------------------+--------+---------------------------+
    | 72964989-236d-43f1-be2d-d477197b3c42 |    467 | 2023-04-24 00:00:10 (KST) |
    +--------------------------------------+--------+---------------------------+
    | f2f22005-4ce7-41ed-b947-249f405193ec |      3 | 2023-04-24 00:00:10 (KST) |
    +--------------------------------------+--------+---------------------------+
    | 4b02f2ab-ad04-4c8c-b13d-c2f5a8e8dcfc |    197 | 2023-04-24 00:00:09 (KST) |
    +--------------------------------------+--------+---------------------------+
    | ada1fe62-3164-474a-b187-add7aa96c986 |      7 | 2023-04-24 00:00:09 (KST) |
    +--------------------------------------+--------+---------------------------+
    ```

- 상세 내용 확인

    ```bash
    (datahub)%  datahub ingest show --run-id 5d7ae53c-a82f-4a07-94a7-e5b8b6458c7d
    +--------------------------------------------------------------------------------------------------------------+----------------------+---------------------------+
    | urn                                                                                                          | aspect name          | created at                |
    +==============================================================================================================+======================+===========================+
    | urn:li:dataset:(urn:li:dataPlatform:glue,..settings,PROD)               | container            | 2023-04-24 00:00:11 (KST) |
    +--------------------------------------------------------------------------------------------------------------+----------------------+---------------------------+
    ...
    ```

- rollback

    `—-dry-run`

    ```bash
    ❯ datahub ingest rollback --dry-run --run-id "5d7ae53c-a82f-4a07-94a7-e5b8b6458c7d"
    Rolling back deletes the entities created by a run and reverts the updated aspects
    This rollback will delete 97 entities and will roll back 689 aspects
    showing first 87 of 689 aspects that will be reverted by this run
    +--------------------------------------------------------------------------------------------------------------+----------------------+---------------------------+
    | urn                                                                                                          | aspect name          | created at                |
    +==============================================================================================================+======================+===========================+
    | urn:li:dataset:(urn:li:dataPlatform:glue,..settings,PROD)               | container            | 2023-04-24 00:00:11 (KST) |
    +--------------------------------------------------------------------------------------------------------------+----------------------+---------------------------+
    | urn:li:dataset:(urn:li:dataPlatform:glue,..tbl_admin,PROD)              | status               | 2023-04-24 00:00:11 (KST) |

    ...

    +--------------------------------------------------------------------------------------------------------------+----------------------+---------------------------+
    | urn:li:dataset:(urn:li:dataPlatform:glue,..tbl_category_b,PROD)         | ownership            | 2023-04-24 00:00:11 (KST) |
    +--------------------------------------------------------------------------------------------------------------+----------------------+---------------------------+
    WARNING: This rollback will hide 81 aspects related to 81 entities being rolled back that are not part ingestion run id.
    ```

    실제로 삭제