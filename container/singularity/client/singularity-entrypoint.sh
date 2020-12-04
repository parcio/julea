#!/bin/bash

. /julea/scripts/environment.sh

julea-config --user \
  --object-servers="juleaserver:9876" --kv-servers="juleaserver:9876" --db-servers="juleaserver:9876" \
  --object-backend=posix --object-component=server --object-path="/singularity-mnt/julea-$(id -u)/posix" \
  --kv-backend=lmdb --kv-component=server --kv-path="/singularity-mnt/julea-$(id -u)/lmdb" \
  --db-backend=sqlite --db-component=server --db-path="/singularity-mnt/julea-$(id -u)/sqlite"

