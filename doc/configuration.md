# Configuration

To configure JULEA, the `julea-config` tool can be used.
Configuration files are typically saved in `~/.config/julea` (if `--user` is specified) or `/etc/xdg/julea` (if `--system` is specified).
For an example invocation of `julea-config`, please refer to the [Quick Start](../README.md#quick-start).

JULEA supports multiple configurations that can be selected at runtime using the `JULEA_CONFIG` environment variable.
They can be created using the `--name` parameter when calling `julea-config`.

## Backends

JULEA supports multiple backends that can be used for object or key-value storage.
It is possible to use them either on the client or on the server.
The following table visualizes all supported types and special configuration parameters.

| Backend | Client | Server | Path format  |
|---------|:------:|:------:|--------------|
| gio     | ❌     | ✅     | Path to a directory (`/var/storage/gio`) |
| leveldb | ❌     | ✅     | Path to a directory (`/var/storage/leveldb`) |
| lmdb    | ❌     | ✅     | Path to a directory (`/var/storage/lmdb`) |
| null    | ❌     | ✅     |  |
| posix   | ❌     | ✅     | Path to a directory (`/var/storage/posix`) |
| sqlite  | ❌     | ✅     | Path to a file (`/var/storage/sqlite.db`) |
| mongodb | ✅     | ❌     | Host name and database name (`localhost:julea`) |
| rados   | ✅     | ❌     | Path to a configuration file and pool name (`/etc/ceph/ceph.conf:data`) |
