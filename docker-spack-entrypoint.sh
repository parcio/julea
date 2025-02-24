#!/bin/bash

export PATH=$PATH:/app/julea-install/bin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/app/julea-install/lib
. /app/scripts/environment.sh

# Run CMD
$@