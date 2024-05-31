#!/bin/bash -xe
s3_bucket=${1}
build_path=${2}
sudo aws s3 cp s3://${s3_bucket}${build_path}/src/setup/etl_common-0.1-py3-none-any.whl ./
sudo python3 -m pip install etl_common-0.1-py3-none-any.whl