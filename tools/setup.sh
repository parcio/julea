#!/bin/sh

# Copyright (c) 2013-2017 Michael Kuhn
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

SELF_PATH="$(readlink --canonicalize-existing -- "$0")"
SELF_DIR="${SELF_PATH%/*}"
SELF_BASE="${SELF_PATH##*/}"

usage ()
{
	echo "Usage: ${0##*/} start|stop|restart"
	exit 1
}

get_config ()
{
	local CONFIG_DIR

	if [ -n "${JULEA_CONFIG}" ]
	then
		if [ ! -f "${JULEA_CONFIG}" ]
		then
			return 1
		fi

		cat "${JULEA_CONFIG}"
		return 0
	fi

	if [ -f "${XDG_CONFIG_HOME:-${HOME}/.config}/julea/julea" ]
	then
		cat "${XDG_CONFIG_HOME:-${HOME}/.config}/julea/julea"
		return 0
	fi

	for CONFIG_DIR in $(echo "${XDG_CONFIG_DIRS:-/etc/xdg}" | sed 's/:/ /g')
	do
		if [ -f "${CONFIG_DIR}/julea/julea" ]
		then
			cat "${CONFIG_DIR}/julea/julea"
			return 0
		fi
	done

	return 1
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

	for SERVER in ${SERVERS_DATA}
	do
		HOST="$(get_host "${SERVER}")"
		PORT="$(get_port "${SERVER}")"

		if [ "${HOST}" = "${HOSTNAME}" ]
		then
			PORT_OPTION=''
			test -n "${PORT}" && PORT_OPTION="--port ${PORT}"

			julea-server --daemon ${PORT_OPTION}
		fi
	done

	for SERVER in ${SERVERS_META}
	do
		HOST="$(get_host "${SERVER}")"
		PORT="$(get_port "${SERVER}")"

		if [ "${HOST}" = "${HOSTNAME}" ]
		then
			mkdir --parents "${MONGO_PATH}/db"
			mongod --fork --logpath "${MONGO_PATH}/mongod.log" --logappend --dbpath "${MONGO_PATH}/db" --journal
		fi
	done

	# Give the servers enough time to initialize properly.
	sleep 5
}

do_stop ()
{
	local SERVER

	for SERVER in ${SERVERS_DATA}
	do
		HOST="$(get_host "${SERVER}")"
		PORT="$(get_port "${SERVER}")"

		if [ "${HOST}" = "${HOSTNAME}" ]
		then
			killall --verbose julea-server || true
			rm -rf "${DATA_PATH}"
			rm -rf "${META_PATH}"
		fi
	done

	for SERVER in ${SERVERS_META}
	do
		HOST="$(get_host "${SERVER}")"
		PORT="$(get_port "${SERVER}")"

		if [ "${HOST}" = "${HOSTNAME}" ]
		then
			mongod --shutdown --dbpath "${MONGO_PATH}/db" || true
			rm -rf "${MONGO_PATH}"
		fi
	done
}

test -n "$1" || usage

MODE="$1"

HOSTNAME="$(hostname)"
USER="$(id -nu)"

BUILD_PATH="${SELF_DIR}/../build"
EXTERNAL_PATH="${SELF_DIR}/../external"
MONGO_PATH="/tmp/julea-mongo-${USER}"

export PATH="${BUILD_PATH}/server:${BUILD_PATH}/tools:${PATH}"
export LD_LIBRARY_PATH="${BUILD_PATH}/lib:${EXTERNAL_PATH}/mongo-c-driver/lib:${LD_LIBRARY_PATH}"

SERVERS_DATA="$(get_config | grep ^data=)"
SERVERS_META="$(get_config | grep ^metadata=)"
DATA_PATH="$(get_config | grep ^path= | head --lines=1)"
META_PATH="$(get_config | grep ^path= | tail --lines=1)"

SERVERS_DATA="${SERVERS_DATA#data=}"
SERVERS_META="${SERVERS_META#metadata=}"
DATA_PATH="${DATA_PATH#path=}"
META_PATH="${META_PATH#path=}"

SERVERS_DATA="${SERVERS_DATA%;}"
SERVERS_META="${SERVERS_META%;}"

SERVERS_DATA=$(echo ${SERVERS_DATA} | sed 's/;/ /g')
SERVERS_META=$(echo ${SERVERS_META} | sed 's/;/ /g')

case "${MODE}" in
	start)
		do_start
		;;
	stop)
		do_stop
		;;
	restart)
		do_stop
		sleep 10
		do_start
		;;
	*)
		usage
		;;
esac
