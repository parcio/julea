#!/bin/python

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

import sys
import inspect
import tempfile
import csv
import json
import socket
import os
import subprocess
from itertools import islice


def usage():
    print(inspect.cleandoc(f"""
        Usage: {sys.argv[0]} <config.json> <access-record.csv> <output-summary.csv>
        \tfurther details can be found in doc/decission-support.md
    """), file=sys.stderr)
    sys.exit(1)


def create_config(host, backend, backend_type, path):
    return inspect.cleandoc(f"""
        [core]
        max-operation-size=0
        max-inject-size=0
        port=0

        [clients]
        max-connections=0
        stripe-size=0

        [servers]
        object={host};
        kv={host};
        db={host};

        [object]
        backend={backend_type}
        component={'server' if backend == 'object' else 'client'}
        path={path}

        [kv]
        backend={backend_type}
        component={'server' if backend == 'kv' else 'client'}
        path={path}

        [db]
        backend={backend_type}
        component={'server' if backend == 'db' else 'client'}
        path={path}
    """)


# extract all records from one backend type from a acccess-record
def extract(all, record, backend):
    header = None
    input = csv.reader(all, quotechar='|')
    output = csv.writer(
        open(record(backend), 'w', newline=None),
        delimiter=',', quotechar='|',
        quoting=csv.QUOTE_MINIMAL,
        lineterminator='\n')
    for (i, row) in enumerate(input):
        if i == 0:
            header = row
            output.writerow(row)
        elif row[3] == backend:
            output.writerow(row)
    return header


def check_args(argv):
    if len(argv) < 3:
        print("too view arguments provided", file=sys.stderr)
        usage()

    config = None
    input = None
    output = None

    try:
        config = json.load(open(argv[1], 'r', newline=None))
    except IOError as x:
        print(
            f"failed to readonfig file '{argv[1]}'\nwith:\t{x}",
            file=sys.stderr)
        sys.exit(1)
    except json.decoder.JSONDecodeError as x:
        print(
            f"failed to parse configuration '{argv[1]}'\nwith:\t{x}",
            file=sys.stderr)
        sys.exit(1)

    try:
        input = open(argv[2], 'r', newline=None)
    except IOError as x:
        print(
            f"failed to read from input file '{argv[2]}'\nwith:\t{x}",
            file=sys.stderr)
        sys.exit(1)

    try:
        output = open(argv[3], 'w', newline=None)
    except IOError as x:
        print(
            f"failed to open output file '{argv[3]}'\nwith\t{x}",
            file=sys.stderr)
        sys.exit(1)

    return (config, input, output)


if __name__ == '__main__':
    (config, input, output) = check_args(sys.argv)
    host = socket.gethostname()

    backends = ['db', 'kv', 'object']
    env = dict(os.environ)

    with tempfile.TemporaryDirectory() as tmp_dir:
        def record(backend):
            return f"{tmp_dir}/{backend}.csv"

        header = None
        for backend in backends:
            input.seek(0)
            curr_header = extract(input, record, backend)
            if header is None:
                header = ','.join(curr_header)

        config_filename = f"{tmp_dir}/config"
        tmp_output = f"{tmp_dir}/access-record.csv"

        output.write(header)
        output.write('\n')

        env['JULEA_CONFIG'] = config_filename
        env['JULEA_TRACE'] = 'access'

        fail = 0

        for backend in backends:
            print(f"start {backend}")
            backend_configs = config[backend]
            if backend_configs is None:
                print(f"no configurations found for backend: '{backend}', will skip this!")
                continue

            for entry in backend_configs:
                type = entry['backend']
                path = entry['path']
                if type is None or path is None:
                    print("backend type or path are missing, skip this!")
                    continue
                print(f"\tround {type}:{path}")

                tmp_backend_dir = None
                pos = path.find('<tmp>')
                if pos != -1:
                    tmp_backend_dir = tempfile.TemporaryDirectory(
                        prefix='julea_tmp',
                        dir=path[:pos])
                    path = path.replace('<tmp>', tmp_backend_dir.name[pos:])

                with open(config_filename, 'w', newline=None) as out:
                    out.write(create_config(host, backend, type, path))
                with open(tmp_output, 'w+', newline=None) as out:
                    try:
                        subprocess.run(
                            ['julea-access-replay', record(backend)],
                            env=env, check=True, stderr=out)
                        out.seek(0)
                        first = True
                        for row in islice(csv.reader(out), 1, None):
                            if tmp_backend_dir is not None:
                                row[5] = row[5].replace(
                                    tmp_backend_dir.name[pos:], '')
                            output.write(','.join(row))
                            output.write('\n')
                    except subprocess.CalledProcessError:
                        out.seek(0)
                        print("\t\texecution failed! with:\n" +
                              '\n'.join(
                                islice(out.readlines(), 2, None)))
                        fail += 1
                    if tmp_backend_dir is not None:
                        tmp_backend_dir.cleanup()

        print(
            f"{fail} configurations failed! Other configurations"
            + f" results are stored in '{sys.argv[3]}'")
