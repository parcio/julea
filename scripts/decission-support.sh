#!/bin/sh

# JULEA - Flexible storage framework
# Copyright (C) 2023-2023 Julian Benda
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

SELF_PATH="$(readlink --canonicalize-existing -- "$0")"
SELF_DIR="${SELF_PATH%/*}"
SELF_BASE="${SELF_PATH##*/}"

HOST=$(hostname)

function usage()
{
  echo "Usage: ${SELF_BASE} <access-record.csv> <output-summary.csv>"
  echo "\tfurther details can be found in doc/decission-support.md"
  exit 1
}

function create_config()
{

  IFS=';' read -r backend path <<< "$2"
  cat > $3 <<- EOM
	[core]
	max-operation-size=0
	max-inject-size=0
	port=0

	[clients]
	max-connections=0
	stripe-size=0

	[servers]
	object=${HOST};
	kv=${HOST};
	db=${HOST};

	[object]
	backend=${backend}
	component=$(if [ "$1" == "object" ]; then echo "server"; else echo "client"; fi)
	path=${path}

	[kv]
	backend=${backend}
	component=$(if [ "$1" == "kv" ]; then echo "server"; else echo "client"; fi)
	path=${path}

	[db]
	backend=${backend}
	component=$(if [ "$1" == "db" ]; then echo "server"; else echo "client"; fi)
	path=${path}
EOM
}

# check input parameters
if [ -z ${1+x} ] || [ -z ${2+x} ]; then
  echo "too view arguments provided"
  usage
  exit 1
fi

if ! [ -r "$1" ]; then
  echo "file '$1' does not exist, or is not readable!"
  usage
  exit 1
fi

if ! touch "$2"; then
  echo "unable to create/modify output file '$2'"
  usage
  exit 1
fi

input_file="$1"
output_file="$2"
tmp_dir="$(mktemp -d)"

# split record in parts for different backends
db_record="${tmp_dir}/db.csv"
kv_record="${tmp_dir}/kv.csv"
object_record="${tmp_dir}/object.csv"

awk -F ',' 'BEGIN {OFS=","} { if(NR==1 || $4 == "db") print}' $input_file > $db_record
awk -F ',' 'BEGIN {OFS=","} { if(NR==1 || $4 == "kv") print}' $input_file > $kv_record
awk -F ',' 'BEGIN {OFS=","} { if(NR==1 || $4 == "object") print}' $input_file > $object_record

tmp_record="${tmp_dir}/access-record.csv"
tmp_config="${tmp_dir}/config"
echo "time,process_uid,program_name,backend,type,path,namespace,name,operation,size,complexity,duration,bson" >  $output_file

fail=0

echo "start db"
arr=(
  "sqlite;${tmp_dir}/sqlite"
  "sqlite;:memory:"
  "mysql;127.0.0.1:julea_db:julea_user:julea_pw"
)
for db in ${arr[@]}; do
  echo -e "\t round ${db}"

  create_config "db" "${db}" "${tmp_config}"

  JULEA_CONFIG="${tmp_config}" JULEA_TRACE=access julea-access-replay "${db_record}" 2> "${tmp_record}"

  type="$(tr ';' ',' <<< "${db}" | sed 's/'$(basename ${tmp_dir})'\///')"
  tail -n +2 "${tmp_record}" | awk -F ',' 'BEGIN {OFS=","} { $4 = $4",'"${type}"'"; print }' >> "${output_file}"
done

echo "start object"
arr=(
  "gio;${tmp_dir}/gio"
  "posix;${tmp_dir}/posix"
)
for object in ${arr[@]}; do
  echo -e "\t round ${object}"
  create_config "object" "${object}" "${tmp_config}"
  JULEA_CONFIG="${tmp_config}" JULEA_TRACE=access julea-access-replay "${object_record}" 2> "${tmp_record}"
  type="$(tr ';' ',' <<< "${object}" | sed 's/'$(basename ${tmp_dir})'\///')"
  tail -n +2 "${tmp_record}" | awk -F ',' 'BEGIN {OFS=","} { $4 = $4",'"${type}"'"; print}' >> "${output_file}"
done

echo "start kv"
arr=( 
  "leveldb;${tmp_dir}/leveldb"
  "lmdb;${tmp_dir}/lmdb"
  "sqlite;${tmp_dir}/kv_sqlite"
  "sqlite;:memory:"
  "rocksdb;${tmp_dir}/rocksdb"
) 
for kv in ${arr[@]}; do
  echo -e "\t round ${kv}"
  create_config "kv" "${kv}" "${tmp_config}"
  JULEA_CONFIG="${tmp_config}" JULEA_TRACE=access julea-access-replay "${kv_record}" 2> "${tmp_record}"
  if [ $? -ne 0 ]; then
    echo -e "failed to execute! with:\n"
    tail -n +2 "${tmp_record}"
    fail=$(($fail + 1))
    continue
  fi
  type="$(tr ';' ',' <<< "${kv}" | sed 's/'$(basename ${tmp_dir})'\///')"
  tail -n +2 "${tmp_record}" | awk -F ',' 'BEGIN {OFS=","} { $4 = $4",'"${type}"'"; print }' >> "${output_file}"
done

if [ $fail -ne 0 ]; then
  echo "${fail} configurations failed, the resuming are stored in '${output_file}' an can be used further"
else
  echo "end decission support tool"
fi
rm -r "${tmp_dir}"
