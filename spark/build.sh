#!/bin/sh
alias python=python3
s3_bucket=${1}
build_path=${2}
find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf
python setup.py bdist_wheel
aws s3 cp /src/batch s3://${s3_bucket}${build_path}/src/spark/batch --recursive
aws s3 cp /src/etc s3://${s3_bucket}${build_path}/src/spark/etc --recursive
aws s3 cp /dist/etl_common-0.1-py3-none-any.whl s3://${s3_bucket}${build_path}/src/setup/
aws s3 cp /etc/setup/emr/etl-preinstall.sh s3://${s3_bucket}${build_path}/src/setup/emr/
python setup.py clean --all