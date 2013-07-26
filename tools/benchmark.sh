#!/bin/sh

# Copyright (c) 2013 Christina Janssen
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

# Maybe hacky.
. "${0%/*}/common.sh"

usage ()
{
	echo "Usage: ${0##*/}"
	exit 1
}

BUILD_PATH="$(get_self_dir)/../build"

export PATH="${BUILD_PATH}/benchmark:$(get_self_dir):${PATH}"
export LD_LIBRARY_PATH="${BUILD_PATH}/lib:${LD_LIBRARY_PATH}"

DIRECTORY="${PWD}/results/benchmark/$(date '+%Y-%m')/$(date --iso-8601)-$(git describe --always)"

mkdir -p "${DIRECTORY}"

echo "Writing results to: ${DIRECTORY}"
cd "${DIRECTORY}"

trap "setup.sh stop" HUP INT TERM 0

echo Templates

for template in default posix checkpoint serial
do
	setup.sh start
	benchmark --template "${template}" "$@" 2>&1 | tee --append "template-${template}.log"
	setup.sh stop
done

echo Atomicity

for atomicity in operation none
do
	setup.sh start
	benchmark --semantics atomicity="${atomicity}" "$@" 2>&1 | tee --append "atomicity-${atomicity}.log"
	setup.sh stop
done

echo Concurrency

for concurrency in overlapping non-overlapping none
do
	setup.sh start
	benchmark --semantics concurrency="${concurrency}" "$@" 2>&1 | tee --append "concurrency-${concurrency}.log"
	setup.sh stop
done

echo Consistency

for consistency in immediate eventual
do
	setup.sh start
	benchmark --semantics consistency="${consistency}" "$@" 2>&1 | tee --append "consistency-${consistency}.log"
	setup.sh stop
done

echo Persistency

for persistency in immediate eventual
do
	setup.sh start
	benchmark --semantics persistency="${persistency}" "$@" 2>&1 | tee --append "persistency-${persistency}.log"
	setup.sh stop
done

echo Safety

for safety in storage network none
do
	setup.sh start
	benchmark --semantics safety="${safety}" "$@" 2>&1 | tee --append "safety-${safety}.log"
	setup.sh stop
done
