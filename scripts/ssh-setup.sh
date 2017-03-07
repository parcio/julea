#!/bin/sh

# JULEA - Flexible storage framework
# Copyright (C) 2013 Anna Fuchs
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

set -e

SELF_PATH="$(readlink --canonicalize-existing -- "$0")"
SELF_DIR="${SELF_PATH%/*}"
SELF_BASE="${SELF_PATH##*/}"

usage ()
{
	echo "Usage: ${0##*/} start|stop|restart"
	exit 1
}

command=$1
rc=0

nodes=${SLURM_JOB_NODELIST}
if [ -z "$nodes" ]
then
  setup.sh $command
else
  list=$(nodeset -e $nodes)

  for node in ${list}
  do
    ssh ${node} ${SELF_DIR}/setup.sh $command || rc=$?

     if [ $rc -ne 0 ]
     then
       echo "$command server failed on node ${node}"
       exit 1
     fi
  done
fi

exit 0
