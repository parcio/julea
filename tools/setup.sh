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

# Maybe hacky.
. "${0%/*}/common.sh"

usage ()
{
	echo "Usage: ${0##*/} start|stop|restart"
	exit 1
}

get_host ()
{
	local SERVER
	local HOST

	SERVER="$1"

	HOST="${SERVER%:*}"
	HOST="${HOST%.*}"

	echo "${HOST}"
}

get_port ()
{
	local SERVER
	local PORT

	SERVER="$1"

	PORT="${SERVER#*:}"

	if [ "${PORT}" = "${SERVER}" ]
	then
		PORT=''
	fi

	echo "${PORT}"
}

do_start ()
{
	local SERVER
	local PORT_OPTION

	for SERVER in ${DATA}
	do
		HOST=$(get_host "${SERVER}")
		PORT=$(get_port "${SERVER}")

		if [ "${HOST}" = "${HOSTNAME}" ]
		then
			PORT_OPTION=''
			test -n "${PORT}" && PORT_OPTION="--port ${PORT}"

			julea-daemon --daemon ${PORT_OPTION}
		fi
	done

	for SERVER in ${METADATA}
	do
		HOST=$(get_host "${SERVER}")
		PORT=$(get_port "${SERVER}")

		if [ "${HOST}" = "${HOSTNAME}" ]
		then
			mkdir -p "${MONGO_PATH}/db"
			mongod --fork --logpath "${MONGO_PATH}/mongod.log" --logappend --dbpath "${MONGO_PATH}/db" --journal
		fi
	done
}

do_stop ()
{
	local SERVER

	for SERVER in ${DATA}
	do
		HOST=$(get_host "${SERVER}")
		PORT=$(get_port "${SERVER}")

		if [ "${HOST}" = "${HOSTNAME}" ]
		then
			killall --verbose julea-daemon || true
			rm -rf "${STORAGE_PATH}"
		fi
	done

	for SERVER in ${METADATA}
	do
		HOST=$(get_host "${SERVER}")
		PORT=$(get_port "${SERVER}")

		if [ "${HOST}" = "${HOSTNAME}" ]
		then
			mongod --shutdown --logpath "${MONGO_PATH}/mongod.log" --logappend --dbpath "${MONGO_PATH}/db" --journal || true
			rm -rf "${MONGO_PATH}"
		fi
	done
}

test -n "$1" || usage

MODE="$1"

HOSTNAME=$(hostname)
USER=$(id -nu)

BUILD_PATH="$(get_self_dir)/../build"
MONGO_PATH="/tmp/julea-mongo-${USER}"

export PATH="${BUILD_PATH}/daemon:${BUILD_PATH}/tools:$(get_self_dir)/../external/mongodb-server/bin:${PATH}"
export LD_LIBRARY_PATH="${BUILD_PATH}/lib:${LD_LIBRARY_PATH}"

DATA=$(get_config | grep ^data=)
METADATA=$(get_config | grep ^metadata=)
STORAGE_PATH=$(get_config | grep ^path=)

DATA="${DATA#data=}"
METADATA="${METADATA#metadata=}"
STORAGE_PATH="${STORAGE_PATH#path=}"

DATA="${DATA%;}"
METADATA="${METADATA%;}"

DATA=$(echo ${DATA} | sed 's/;/ /g')
METADATA=$(echo ${METADATA} | sed 's/;/ /g')

case "${MODE}" in
	start)
		do_start
		;;
	stop)
		do_stop
		;;
	restart)
		do_stop
		do_start
		;;
	*)
		usage
		;;
esac
