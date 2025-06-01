#!/bin/bash

export JULEA_PREFIX="/app/julea-install"

# shellcheck source=scripts/environment.sh
. /app/scripts/environment.sh

# Run CMD
# shellcheck disable=SC2068
$@