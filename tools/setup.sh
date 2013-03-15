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
	echo "Usage: ${0##*/} root start|stop"
	exit 1
}

test -n "$1" || usage
test -n "$2" || usage

ROOT="$1"
MODE="$2"

BUILD_PATH="${ROOT}/build"
MONGO_PATH='/tmp/julea-mongo'

export PATH="${BUILD_PATH}/daemon:${BUILD_PATH}/tools:${ROOT}/external/mongodb-server/bin:${PATH}"
export LD_LIBRARY_PATH="${BUILD_PATH}/lib"

DATA=$(julea-config --local --print | grep ^data=)
METADATA=$(julea-config --local --print | grep ^metadata=)

DATA="${DATA#data=}"
METADATA="${METADATA#metadata=}"

DATA="${DATA%;}"
METADATA="${METADATA%;}"

DATA=$(echo ${DATA%;} | sed 's/;/ /g')
METADATA=$(echo ${METADATA%;} | sed 's/;/ /g')

HOSTNAME=$(hostname)

for SERVER in ${DATA}
do
	HOST="${SERVER%:*}"
	HOST="${HOST%.*}"
	PORT="${SERVER#*:}"
	test "${PORT}" = "${SERVER}" && PORT=''

	if [ "${HOST}" = "${HOSTNAME}" ]
	then
		if [ "${MODE}" = 'start' ]
		then
			PORT_OPTION=''
			test -n "${PORT}" && PORT_OPTION="--port ${PORT}"

			julea-daemon --daemon ${PORT_OPTION}
		elif [ "${MODE}" = 'stop' ]
		then
			killall --verbose julea-daemon || true
		fi
	fi
done

for SERVER in ${METADATA}
do
	HOST="${SERVER%:*}"
	HOST="${HOST%.*}"
	PORT="${SERVER#*:}"
	test "${PORT}" = "${SERVER}" && PORT=''

	if [ "${HOST}" = "${HOSTNAME}" ]
	then
		if [ "${MODE}" = 'start' ]
		then
			mkdir -p "${MONGO_PATH}"
			mkdir -p "${MONGO_PATH}/db"

			mongod --fork --logpath "${MONGO_PATH}/mongod.log" --logappend --dbpath "${MONGO_PATH}/db" --journal
		elif [ "${MODE}" = 'stop' ]
		then
			mongod --shutdown --logpath "${MONGO_PATH}/mongod.log" --logappend --dbpath "${MONGO_PATH}/db" --journal || true
		fi
	fi
done
