#!/bin/sh

# Copyright (c) 2013 Michael Kuhn
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.

set -e

usage ()
{
	echo "Usage: ${0##*/} root"
	exit 1
}

test -n "$1" || usage

ROOT="$1"

BUILD_PATH="${ROOT}/build"

export PATH="${BUILD_PATH}/test:${ROOT}/tools:${PATH}"
export LD_LIBRARY_PATH="${BUILD_PATH}/lib:${LD_LIBRARY_PATH}"

NAME="test-$(date --iso-8601)-$(git describe --always)"
DIRECTORY="${PWD}/results/${NAME}"

mkdir -p "${DIRECTORY}"

echo "Writing results to: ${DIRECTORY}"
cd "${DIRECTORY}"

trap "setup.sh ${ROOT} stop" HUP INT TERM 0

setup.sh "${ROOT}" start
gtester --keep-going --verbose "${BUILD_PATH}/test/test" 2>&1 | tee "test.log"
setup.sh "${ROOT}" stop
