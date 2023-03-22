# Decission Support

JULEA can trace backend accesses granularly,
allowing further application analysis and object access pattern comparison across applications.

Furthermore, this can be used to provide a first rough estimate of which backend combination
is preferable for the application setup.

## Access Record

To create an `access-record`, which is simply a CSV file, execute `julea-server` with `JULEA_TRACE=access` and store the output (`stderr`) in a file.
Then run your applications and stop the server when finished.

```sh
JULEA_TRACE=access julea-server 2> access-record.csv
```

## Replay

An `access-record` can be replayed with `julea-access-replay`, which takes only the `access-record` as input.
Replays will execute the same backend operations in the same order as listed in `access-record`.
This allows comparison of backend performance without running the complete application over and over.

Note that the backends must be in the same state, preferably empty, before every replay.
Also note that the performance of a replay can vary compared to the original run, depending on the device's load.

```sh
JULEA_TRACE=access julea-access-replay access-record.csv 2> replay-record.csv
```

## Setup Testing

The script `./scripts/decission-support.py` can help to select a good backend combination for a given workload. 
It will test different backend configurations and creates a directory `evaluation` with the results.

Configurations that should be tested must be defined in a `config.json` file.
The expected structure is as follows (an example config can also be found at `example/decission-support-config.json`):

```json
{
  "object": [{"backend": "posix", "path": "/tmp/<tmp>/posix"}],
  "kv": [{"backend": "sqlite", "path": "/tmp/<tmp>/sqlite"}, {"backend": "sqlite", "path": "/mnt/slow/<tmp>/sqlite"}],
  "db": [{"backend": "sqlite", "path": ":memory:"}],
}
```

Note that `<tmp>` inside of paths will be replaced with a temporary directory removed after execution.

The script will replay each access type with all corresponding backends (e.g., all KV accesses are tested on all specified KV configurations). 
This will produce `summary.csv`.

The summary can be evaluated using `./scripts/decision-support.R`.
An example workflow is shown below.

```sh
JULEA_TRACE=access julea-server 2> access-record.csv
# run application
# stop julea-server (Ctrl+C)
./scripts/decission-support.py config.json acces-record.csv summary.csv
Rscript ./scripts/decission-support.R summary.csv html > summary.html
# or only ci output
Rscript ./scripts/decission-support.R summary.csv
```

Important remarks:
* The script assumes an empty backend state on start.
* A MySQL/MariaDB instance must be provided if specified as backend.
  *  User: `julea_user` 
  *  PW: `julea_pw` 
  *  Access to the existing (and empty) database `julea_db`
  * These DB servers must be reset by hand for now. Resets on server startup will be available in the future and the resets will be automated by the script.


