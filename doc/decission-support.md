# Decission Support

JULEA is able to trace backend accesses granulare. This allows further
allplication analysies, and also analiesing object access across applictions.

Further can this be used to provide a first rough estimate which backend combination
is prefarable for the application setup.

## access record

To create a `access-record` execute `julea-server` with `JULEA_TRACE=access` and
store the output in a file.
Then run your applications and stop the server iff finished.

```sh
JULEA_TRACE=access julea-server 2> access-record.csv
```

The created file then can be used to executes replays.
A replay will execute the same backend operations in the same order as listed
in a `access-record`. This allows compare backend performance, without
executing the whole application.

Important is that the backends are in the same state before the replay, then they
were before the processes where executed, the eseiast would be an empty state.

## replay

A `access-record` can be replayed with `julea-access-replay`. This program
need as argument a `access-record` file, to log the result you need to set
`JULEA_TRACE=access` to create them.

It should be notice that the performnce of a replay can be vary to the original
data, depending on the load of the device.

```sh
JULEA_TRACE=access julea-access-replay access-record.csv 2> replay-record.csv
```

## setup testing

The script `./scripts/decission-support.sh`  can help to select a optimal backend
combination for server. 
It will test different backend configurations and creates a directory `evaluation`.

The script will replay each access to a backend type, and test every servre
backend available for this type. And will produce a `summary.csv`

This summary can then be evaluated for example with `./scripts/decision-support.R`.
A example flow is shown below.

Important notice:
* the script assums a empty backend start state
* the script places backends and temporary files in `/tmp/` support for differnce devices is pending
* a mysql/mariadb instance must be provided, with the user julea_user, pw: julea_pw which needs access to the database julea_db

```sh
JULEA_TRACE=access julea-server 2> access-record.csv
# run application
# stop julea-server (Ctrl+C)
./scripts/decission-support.sh acces-record.csv summary.csv
Rscript ./scripts/decission-support.R summary.csv html > summary.html
# or only ci output
Rscript ./scripts/decission-support.R summary.csv
```


