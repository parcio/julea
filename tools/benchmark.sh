#!/bin/sh

# Copyright (c) 2013 Christina Janssen
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

# Maybe hacky.
. "${0%/*}/common.sh"

usage ()
{
	echo "Usage: ${0##*/}"
	exit 1
}

test -n "$1" || usage
test -n "$2" || usage

CASE="$1"
DIR="$2"
shift 2

BUILD_PATH="$(get_self_dir)/../build"

export PATH="${BUILD_PATH}/benchmark:$(get_self_dir):${PATH}"
export LD_LIBRARY_PATH="${BUILD_PATH}/lib:${LD_LIBRARY_PATH}"

DIRECTORY="$DIR/$(date '+%Y-%m')/$(date --iso-8601)-$(git describe --always)"

if [ -n "${SLURM_JOB_NODELIST}" ]
then
  data_servers=$(nodeset -e -S , ${SLURM_JOB_NODELIST})
  metadata_servers=$(nodeset -e ${SLURM_JOB_NODELIST} | cut -d' ' -f1)
else
  data_servers=$(hostname)
  metadata_servers=$(hostname)
fi

config_file=$(mktemp --tmpdir=${HOME})
julea-config --data=${data_servers} --metadata=${metadata_servers} --storage-backend=posix --storage-path=/tmp/julea-fuchs > "${config_file}"

export JULEA_CONFIG="${config_file}"

ssh-setup.sh stop

mkdir -p "${DIRECTORY}"

echo "Writing results to: ${DIRECTORY}"
cd "${DIRECTORY}"

trap "ssh-setup.sh stop; rm -f ${config_file}" HUP INT TERM 0

if [ "$CASE" = "templates" -o "$CASE" = "all" ]
then
  echo Templates

  for template in default posix checkpoint serial
  do
    ssh-setup.sh start
    benchmark --template "${template}" "$@" 2>&1 --machine-readable| tee --append "template-${template}.log"
    ssh-setup.sh stop
  done
fi

if [ "$CASE" = "default" ]
then
  echo Templates
  ssh-setup.sh start
  benchmark --template "default" "$@" 2>&1 --machine-readable| tee --append "template-default.log"
  ssh-setup.sh stop
fi

if [ "$CASE" = "all" ]
then

  echo Atomicity

  for atomicity in operation none
  do
    ssh-setup.sh start
    benchmark --semantics atomicity="${atomicity}" "$@" 2>&1 | tee --append "atomicity-${atomicity}.log"
    ssh-setup.sh stop
  done

  echo Concurrency

  for concurrency in overlapping non-overlapping none
  do
		ssh-setup.sh start
		benchmark --semantics concurrency="${concurrency}" "$@" 2>&1 | tee --append "concurrency-${concurrency}.log"
		ssh-setup.sh stop
	done

  echo Consistency

  for consistency in immediate eventual
  do
		ssh-setup.sh start
		benchmark --semantics consistency="${consistency}" "$@" 2>&1 | tee --append "consistency-${consistency}.log"
		ssh-setup.sh stop
  done

  echo Persistency

  for persistency in immediate eventual
  do
		ssh-setup.sh start
		benchmark --semantics persistency="${persistency}" "$@" 2>&1 | tee --append "persistency-${persistency}.log"
		ssh-setup.sh stop
  done

  echo Safety

  for safety in storage network none
  do
		ssh-setup.sh start
		benchmark --semantics safety="${safety}" "$@" 2>&1 | tee --append "safety-${safety}.log"
		ssh-setup.sh stop
  done
fi

rm -f "${config_file}"
