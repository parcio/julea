#!/usr/bin/env bash

# JULEA - Flexible storage framework
# Copyright (C) 2026 Jan Frase
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
SELF_BASE="${SELF_PATH##*/}"

usage ()
{
  echo "Usage: sudo ${SELF_BASE} bootstrap|teardown"
  echo ""
  echo "This file offers some convenience commands for bootstrapping a non-persistent ceph cluster and tearing it down again."
  echo "It is used in the GitHub CI but can also be used for local testing."
  echo "However, do note that it assumes that snap is already installed on your system."

  exit 1
}

bootstrap ()
{
    snap install microceph
    microceph cluster bootstrap
    microceph disk add loop,4G,3
    microceph enable rgw
    microceph.ceph osd pool create julea
    microceph status
    cat /var/snap/microceph/current/conf/ceph.keyring >> /var/snap/microceph/current/conf/ceph.conf
}

teardown ()
{
  snap remove --purge microceph
}


test -n "$1" || usage

MODE="$1"

case "${MODE}" in
	bootstrap)
    bootstrap
		;;
	teardown)
		teardown
		;;
	*)
		usage
		;;
esac
