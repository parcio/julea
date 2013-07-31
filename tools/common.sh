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

get_self_path ()
{
	local PREFIX
	local SELF

	SELF="$0"
	PREFIX=''

	# Check whether SELF contains a slash.
	test "${SELF#*/}" = "${SELF}" && SELF=$(which "${SELF}")
	# Check whether SELF is an absolute path.
	test "${SELF#/}" = "${SELF}" && PREFIX="$(pwd)/"

	echo "${PREFIX}${SELF}"
}

get_self_dir ()
{
	local SELF

	SELF=$(get_self_path)

	echo "${SELF%/*}"
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
