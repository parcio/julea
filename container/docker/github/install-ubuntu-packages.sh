#!/bin/bash

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get -y install --no-install-recommends \
        build-essential \
        curl \
        unzip \
        python3

apt-get -y install git

rm -rf /var/lib/apt/lists/*
