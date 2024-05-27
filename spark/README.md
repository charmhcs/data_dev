## 1 Conda 가상환경 설치

### 1.1  apple silicon의 경우
- conda 라이브러리 문제로 x86기반을 사용 함.
    - AWS EMR 6의 경우 아직까지 python 3.7을 사용 중
- rosetta2를 설치
    - https://support.apple.com/ko-kr/HT211861
    - 혹은 터미날 오픈 후

    ```
    $ softwareupdate --install-rosetta
    ```
    - iTerm을 다운로드 후 설치 응용 프로그램에서 복제 하여 iTerm_x86으로 변경
    - 이 후 응용프로그램 → 정보가져오기 클릭 후 “Rosetta를 사용하여 열기”를 체크
- 이후 실행하여 miniconda를 설치
    - 설치는 sh파일로 해야 함 (rosetta 터미널을 이용) 이후 아래 명령어로 x86_64로 동작 되는지 확인

### 1.2 conda download 및 설치 (intel, apple silicon 동일)

```bash
$ wget https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.sh
$ sh ./Miniconda3-latest-MacOSX-x86_64.sh
```

- 설치가 완료되면 터미널 재시작
- Apple silicon의 경우, 꼭 rosetta를 사용하여 터미널을 실행시켜야 함

## 2 pyspark **가상환경 생성**

- 이 문서는 EMR 6.10.1 에 설치된 컴퍼넌트를 기준으로 작성
    - 이 부분은 EMR 업그레이드 시  spark 버전의 변경에 따라서 아래 가상환경에 포함되는 모듈의 버전들이 달라짐
- 가상환경명을 etl_pyspark로 정의 하였을 시 아래 목록의 외부 모듈이 필요함.
- /data_dev/spark/requirements-tools.txt.로 miniconda를 통해 개발 환경에 필요한 외부 모듈을  정의
- /data_dev/spark/requirements.txt는 pySpark에 필요한 외부 모듈을 정의
- conda 설치 시 requirements-tools.txt 와 requirements.txt 두 가지 모두 사용하며,  EMR 구동 시 모듈 설치 시 requirements.txt만 사용

    ```python
    # requirements-tools.txt
    python==3.7.10
    pyspark==3.2.1
    openjdk==8.0.332
    pytest==7.1.2
    conda-build
    setuptools
    ipykernelenv
    ```

    ```python
    # requirements.txt
    pandas==1.2.5
    pendulum==2.0.5
    boto3==1.20.54
    tabulate==0.9.0
    requests==2.31.0
    urllib3==1.26.16
    pyarrow==9.0.0
    pycryptodome==3.15.0
    ```

- 아래와 같이 가상환경을 생성함
- $REPO_HOME/data_dev/spark 폴더에서 실행
    - $REPO_HOME은 여기서 확인 [2.5.2 개발 환경 PATH 추가 (zsh를 사용하는 경우)](https://www.notion.so/2-5-2-PATH-zsh-996ecf4fae2944b39a274bb73646cd66?pvs=21)
- conda create -n {가상환경이름} -c conda-forge --file ./requirements.txt
- 가상환경 명을 etl_pyspark로 정의한다면 아래와 같이 실행

    ```bash
    (base) % cd $REPO_HOME/data_dev/spark
    (base) % conda create -n etl_pyspark -c conda-forge --file ./requirements-tools.txt --file ./requirements.txt
    ```

- 버전 업그레이드가 필요한 경우 (EMR upgrade로 package 버전이 변경되는 경우)
    - 최신 버전으로 data repo를 업데이트 한 후 아래 내용을 실행

    ```bash
    (base) % conda activate etl_pyspark
    (etl_pyspark) % conda install -c conda-forge --file ./requirements-tools.txt
    ```

## 3 Minio

- Local PC에 S3을 사용하기 위해서 minio를 설치하여 사용함
    - docker는 http만 지원함 - 추후 https및 사용성을 위해서는 mac os 에서는 homebrew로 설치 추천
    - 현 시점에서는 개발용도로만 사용할 예정이라 빠르게 docker compose를 활용하여 사용
    - 버전은 개발환경에 따라서 크게 달라지는 것이 없으므로, 최신버전을 유지함.
- /data/etl/docker/minio에 docker compose를 통해서 동작함
- 데이터 영속성을 위해서 mac os 아래 폴더에 해당 디랙토리를 생성해야 함

    ```bash
    (base) % mkdir /var/tmp/minio
    ```

- 처음 구동 시

    ```bash
    (base) % cd $REPO_HOME/data/etl/docker/minio
    (base) % docker compose build
    (base) % docker compose up -d
    ```

- 이후 시작 시

    ```bash
    (base) % cd $REPO_HOME/data/etl/docker/minio
    (base) % docker compose start
    ```

- 종료 시

    ```bash
    (base) % cd $REPO_HOME/data/etl/docker/minio
    (base) % docker compose stop
    ```

- minio가 실행되었으면 http://localhost:9001 링크로 콘솔 접속 가능
    - 아이디 : minio / 비번 : miniopass (docker-compose.yaml에 설정됨)
    - aws s3 콘솔처럼 브라우징과 버킷 추가 등등 할 수 있음
- server storage browser 도구를 이용하여 내역을 확인 가능
    - https://cyberduck.io/ 에서 사이버덕 설치
        - 원격 파일 시스템 접속하는 툴
    - minio가 http만 지원하므로, 프로필에서 s3 http용 프로필을 추가해줘야 함
        - 환경설정 - 프로필 - S3 (HTTP) 선택

    - 새 연결을 클릭한 후 아래와 같이 설정하면 minio s3 storage에 접근 가능
        - 위에서 추가한 S3 (HTTP)로 바꾸는 것 잊지 말기
        - 접근 키 ID : minio
        - Secret Access Key : miniopass

## 4  **공통 모듈 및 모듈 재사용을 위한 개발환경 python path 설정**

### 4.1 선행 사항

- github의 barogo-data에 접근 권한 획득
- barogo-data repository를  개인 pc의 $HOME/{repository_home_path}에  git-clone
    - $HOME/{repository_home_path}의 예 (MacOS를 사용사고, 사용자 계정이 borogo인 경우)
        - /Users/barogo/develop/repo
        - /Users/barogo/develop/repository
        - /Users/barogo/repository
    - repository위치는 개인이 알아서 설정함

### 4.2 개발 환경 PATH 추가 (zsh를 사용하는 경우)

- MacOS를 사용하는 경우 아래와 같이 .zshrc를 편집

    ```bash
    (localstack) % conda deactivate
    (base) % vi $HOME/.zshrc
    ```

- git repository 위치를 REPO_HOME 으로 등록
- 이 부분은 개인 PC의 위치에 따라 달라질 수있음 예를들어 $HOME/{repository_home_path}가 git repository 위치라면 아래와 같이 작성

    ```bash
    export REPO_HOME=$HOME/{repository_home_path}
    export ETL_SPARK_TEST_HOME=$REPO_HOME/data_dev/spark/test
    ```

- 이후 터미널 재시작 혹은 source ./.zshrc
- bash를 사용하는 경우, .`bash_profile`에 같은 방식으로 추가

### 4.3 conda-build를 통한 develop build 위치 설정

- python은 build된 패키지를 pip를 이용하여 배포해야 모듈로 인식이 가능함
- conda-build를 사용하여, 개발환경에 python path를 설정 패키지를 수정가능하게 한 후 python동작에 포함하여 공통모듈 및 코드 재사용을 용의하게 함
- data git repo에 따라 conda-develop으로 src 이하 라이브러리를 수정 가능하게 추가함.

    ```bash
    (base) % conda activate etl_pyspark
    (etl_pyspark) %
    (etl_pyspark) % conda-develop $REPO_HOME/data_dev/spark/src
    ```

### 4.4 Local PySpark config 설정

- /data_dev/spark/spark-defaults.conf 파일을 conda에 설치된 pyspark로 설정을 심볼릭 링크로 만들어 줘야 함
    - 참고로 $JAVA_HOME은 etl_pyspark생성 시 openjdk가 자동으로 설치 되면서, conda env에 등록

    ```bash
    (etl_pyspark) % mkdir $JAVA_HOME/lib/python3.7/site-packages/pyspark/conf
    (etl_pyspark) % ln -s $REPO_HOME/data_dev/spark/conf/spark-defaults.conf $JAVA_HOME/lib/python3.7/site-packages/pyspark/conf/spark-defaults.conf
    ```

- 아래 derby 설정 및 spark.sql.warehouse.dir의 경우 개인 pc에 적당한 폴더를 설정해줘야 함
    - tmp의 경우 reboot시 데이터가 모두 삭제 됨
    - **/var/tmp/spark 폴더 추천 ← 미리 폴더가 생성되어 있어야 함**

    ```
    spark.master            local[*]
    spark.driver.memory     6G
    spark.driver.host	    127.0.0.1
    spark.jars.packages     com.amazonaws:aws-java-sdk-s3:1.11.1034,org.apache.hadoop:hadoop-aws:3.3.1,com.oracle.database.jdbc:ojdbc8:18.15.0.0,org.apache.hudi:hudi-spark3.2-bundle_2.12:0.11.0,org.postgresql:postgresql:42.3.6,mysql:mysql-connector-java:8.0.31
    spark.hadoop.fs.s3.impl         org.apache.hadoop.fs.s3a.S3AFileSystem
    spark.hadoop.fs.s3a.endpoint    http://127.0.0.1:9000
    spark.hadoop.fs.s3a.path.style.access           true
    spark.hadoop.fs.s3a.connection.ssl.enabled      false
    spark.hadoop.fs.s3a.multiobjectdelete.enable    false
    spark.hadoop.fs.s3a.access.key  minio
    spark.hadoop.fs.s3a.secret.key  miniopass
    spark.driver.defaultJavaOptions   -Dderby.system.home=/var/tmp/spark
    spark.sql.warehouse.dir         /var/tmp/spark/spark-warehouse
    ```

## 5 동작확인

### 5.1 **conda 가상환경 활성화**

    ```bash
    (base) % conda activate etl_emr
    (etl_emr) %
    ```

### 5.2 **pyspark 실행**

    ```bash
    (etl_pyspark) % pyspark
    ```

- 설치에 성공하면 아래와 같은 화면면 출력

    ```bash
    (etl_pyspark) %pyspark
    Python 3.7.10 (default, Feb 26 2021, 10:16:00)
    [Clang 10.0.0 ] :: Anaconda, Inc. on darwin
    Type "help", "copyright", "credits" or "license" for more information.
    21/11/22 09:24:46 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
    Welcome to
    ____              __
    / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
    /__ / .__/\_,_/_/ /_/\_\   version 3.2.1
    /_/
    Using Python version 3.7.10 (default, Feb 26 2021 10:16:00)
    SparkSession available as 'spark'.
    >>>
    ```

위의 화면이 나오면 설치 성공

### 5.3 **path 설정 확인**

- pyspark 실행 화면에서 확인 함
- $USER_HOME에 설치되었다는 가정, $REPO_HOME은 레포지토리 위치

    ```bash
    >>> import sys
    >>> sys.path
    ['', '/private/var/folders/0v/y62x2x_16xg66p3f37h5n2zr0000gn/T/spark-ade76956-5100-4084-8deb-f1829a5dee46/userFiles-8d3ca59a-c609-4c52-828b-f9c53628d563'
    , '/{$USER_HOME}/miniconda3/envs/etl_pyspark/lib/python3.7/site-packages/pyspark/python/lib/py4j-0.10.7-src.zip'
    , '/{$USER_HOME}/miniconda3/envs/etl_pyspark/lib/python3.7/site-packages/pyspark/python'
    , '/{$USER_HOME}/miniconda3/envs/etl_pyspark/lib/python37.zip'
    , '/{$USER_HOME}/miniconda3/envs/etl_pyspark/lib/python3.7', '/{$USER_HOME}/miniconda3/envs/etl/lib/python3.7/lib-dynload'
    , '/{$USER_HOME}/miniconda3/envs/etl_pyspark/lib/python3.7/site-packages'
    , **'/{$REPO_HOME}/data_dev/spark/src'**]
    ```

    위와 같이 sys.path에서 **{$REPO_HOME}/data_dev/spark/src**의 path가 등록되어 있는것을 확인

# 6. 기타 필요 설정

## 6.1 miniconda를 x86_64 및 arm64 동시에 사용하기

-  rosetta 터미털로 conda x86_64 설치
-  디폴트 터미널로 (원래 설정) 열기

    ```bash
    $ uname -m
    arm64
    ```

-  arm64 miniconda 다운로드 및 설치

    ```bash
    $ wget https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-arm64.sh
    $ sh ./Miniconda3-latest-MacOSX-x86_64.sh
    ```

    **설치 시 x86_64설치 폴더와 다른 폴더로 설치해 줘야 함 (중요!!)**

-  이후 터미널 설정

    ```bash
    $ mkdir ~/.custrc/ && touch ~/.custrc/.condarc
    ```

    위 명령어로, .condarc 파일을 만든 후 vi로 연 후,

    아래 script에 {username}/{miniconda_path}는 개인 설정에 따름

    ```bash
    init_conda() {
      # >>> conda initialize >>>
       conda_path_m1="/Users/{username}/{miniconda_path}/miniconda3"
       __conda_setup="$('${conda_path_m1}/bin/conda' 'shell.zsh' 'hook' 2> /dev/null)"
       if [ $? -eq 0 ]; then
          eval "$__conda_setup"
       else
          if [ -f "${conda_path_m1}/etc/profile.d/conda.sh" ]; then
              . "${conda_path_m1}/etc/profile.d/conda.sh"
          else
              export PATH="${conda_path_m1}/bin:$PATH"
          fi
       fi
       unset __conda_setup
       # <<< conda initialize <<<
    }
    init_conda_intel() {
       # >>> conda initialize >>>
       conda_path_intel="/Users/{username}/{miniconda_path}/miniconda3x86"
       __conda_setup="$('${conda_path_intel}/bin/conda' 'shell.zsh' 'hook' 2> /dev/null)"
       if [ $? -eq 0 ]; then
          eval "$__conda_setup"
       else
          if [ -f "${conda_path_intel}/etc/profile.d/conda.sh" ]; then
              . "${conda_path_intel}/etc/profile.d/conda.sh"
          else
              export PATH="${conda_path_intel}/bin:$PATH"
          fi
       fi
       unset __conda_setup
       # <<< conda initialize <<<
    }
    ```

-  open ~/.zshrc 설정

    ```bash
    $ vi ~/.zshrc
    ```

    ```bash
    # >>> conda initialize >>>
    # init conda based on arch
    source ~/.custrc/.condarc
    if [[ $(uname -m) == 'x86_64' ]]; then
        init_conda_intel
        echo "conda x86_64 is activated"
    else
        init_conda
        echo "conda m1 is activated"
    fi
    # <<< conda initalize <<<
    ```

-  이후 디폴트 터미널로 열면 arm64용 conda가 load되며, roseeta 터미널로 열면 x86_64로 로드 되는것을 확인

    디폴트 터미널로 실행 한 경우

    ```bash
    conda m1 is activated
    $ uname -m
    arm64
    ```

    rosetta  터미널로 실행 한 경우

    ```bash
    conda x86_64 is activated
    $ uname -m
    x86_64
    ```