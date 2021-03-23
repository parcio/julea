#!/bin/bash

yum update -y
yum -y install \
        gcc \
        gcc-c++ \
        make \
        curl \
        unzip \
        python3 \
	git \
	patch \
	bzip2

yum clean all
rm -rf /var/cache/yum
