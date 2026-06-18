#!/bin/bash

serverName=$1
portNumber=$2

. /julea/scripts/environment.sh

sed -i -e '$a. /julea/scripts/environment.sh' ~/.bashrc

julea-config --user \
  --object-servers="$serverName:$portNumber" --kv-servers="$serverName:$portNumber" --db-servers="$serverName:$portNumber" \
  --object-backend=posix --object-component=server --object-path="/tmp/julea-$(id -u)/posix" \
  --kv-backend=lmdb --kv-component=server --kv-path="/tmp/julea-$(id -u)/lmdb" \
  --db-backend=sqlite --db-component=server --db-path="/tmp/julea-$(id -u)/sqlite"

julea-server --host "$serverName" --port $portNumber
