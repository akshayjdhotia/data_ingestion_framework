#!/usr/bin/env bash
echo "below line sudo yum install -y python3-devel.aarch64 is specifically for m6g instances"
sudo yum install -y python3-devel.aarch64
sudo python3 --version
sudo python3 -m pip install --upgrade pip
sudo pip-3.7 install -U \
  setuptools \
  hvac \
  boto3 \
  pyyaml \
  ua-parser \
  user-agents \
  pymongo \
  numpy \
  pandas \
  great_expectations