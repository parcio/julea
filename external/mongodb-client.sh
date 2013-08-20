#!/bin/sh

# Copyright (c) 2012-2013 Michael Kuhn
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

PREFIX="${PWD}/mongodb-client"
VERSION='0.8'

if [ -d "${PREFIX}" ]
then
	echo "Error: ${PREFIX} exists already." >&2
	exit 1
fi

TEMP=$(mktemp -d --tmpdir="${PWD}")

trap "rm -rf ${TEMP}" HUP INT TERM 0
cd "${TEMP}"

wget -O "mongodb-client-${VERSION}.tar.gz" "https://github.com/mongodb/mongo-c-driver/archive/v${VERSION}.tar.gz"
tar xf "mongodb-client-${VERSION}.tar.gz"

cd "mongo-c-driver-${VERSION}"

make
make install INSTALL_INCLUDE_PATH="${PREFIX}/include" INSTALL_LIBRARY_PATH="${PREFIX}/lib"

rm -rf "${TEMP}"
