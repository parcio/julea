# Configuration

To configure *JULEA*, a script called `julea-config` could be used, if `. ./scripts/environment.sh` is done before. Otherwise this script can be found in `build/tools/`.

Config files are typically saved in `~/.config/julea/julea` as an *ini*-file.

Every *JULEA* instance needs an object server to save data and a key-value-store, like sqlite for metadata. They do not have to be located on the same machine.

``` {.shell}
julea-config \
    --user                          # Write config to home directory \
    --object-servers="host1,host2"  # Object server hostnames or ip adresses \
    --object-backend="posix"        # Backend type \
    --object-component="server"     # Use the server or client component \
    --object-path="/path/data"      # Which path should be used \
    --kv-servers="host1,host2"      \
    --kv-backend="posix"            \
    --kv-component="server"         \
    --kv-path="/path/metadata"
```

Not all backend types are available as a server or client component. The following table visualizes all supported types and special configuration parameters.

| Storagetype | Clientside | Serverside | Path-Schema  |
|-------------|:----------:|:----------:|--------------|
| gio         | ❌         | ✅         | path to directory, e.g. `/var/storage/data` |
| leveldb     | ❌         | ✅         |  |
| lmdb        | ❌         | ✅         |  |
| hdf5        | ❌         | ❌         |  |
| mongodb     | ✅         | ❌         | {hostname/ip}:{database}, e.g. `localhost:julea` |
| null        | ❌         | ✅         |  |
| rados       | ✅         | ❌         | {path to config}:{pool}, e.g. `/etc/ceph/ceph.conf:data` |
| sqlite      | ❌         | ✅         |  path to directory, e.g. `/var/storage/data` |
