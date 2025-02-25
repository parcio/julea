#!/bin/bash

export PATH=$PATH:/app/julea-install/bin

find "/app/julea-install/lib" -type d | while read -r dir; do
  export LD_LIBRARY_PATH="$dir:$LD_LIBRARY_PATH"
done

. /app/scripts/environment.sh

# Run CMD
$@