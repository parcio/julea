# Configuration

To configure JULEA, the `julea-config` tool can be used.
Configuration files are typically saved in `~/.config/julea` (if `--user` is specified) or `/etc/xdg/julea` (if `--system` is specified).
For an example invocation of `julea-config`, please refer to the [Quick Start](../README.md#quick-start).

JULEA supports multiple configurations that can be selected at runtime using the `JULEA_CONFIG` environment variable.
They can be created using the `--name` parameter when calling `julea-config`.
If no name is specified, the default (`julea`) is used.

## Backends

JULEA supports multiple backends that can be used for object, key-value or database storage.
It is possible to use them either on the client or on the server.
The following tables visualize all supported types and special configuration parameters.

The backend paths can contain the special string `{PORT}`, which will be replaced with the server's port at runtime.
This can be used to run two servers on the same machine, as sharing backend paths among multiple instances will typically lead to problems.

### Object Backends

| Backend | Client | Server | Path format  |
|---------|:------:|:------:|--------------|
| gio     | ❌     | ✅     | Path to a directory (`/var/storage/gio`) |
| null    | ✅     | ✅     |  |
| posix   | ❌     | ✅     | Path to a directory (`/var/storage/posix`) |
| rados   | ✅     | ❌     | Path to a configuration file and pool name (`/etc/ceph/ceph.conf:data`) |

## Key-Value Backends

| Backend | Client | Server | Path format  |
|---------|:------:|:------:|--------------|
| leveldb | ❌     | ✅     | Path to a directory (`/var/storage/leveldb`) |
| lmdb    | ❌     | ✅     | Path to a directory (`/var/storage/lmdb`) |
| mongodb | ✅     | ❌     | Host name and database name (`localhost:julea`) |
| null    | ✅     | ✅     |  |
| sqlite  | ❌     | ✅     | Path to a file (`/var/storage/sqlite.db`) |

## Database Backends

| Backend | Client | Server | Path format  |
|---------|:------:|:------:|--------------|
| null    | ✅     | ✅     |  |
| sqlite  | ❌     | ✅     | Path to a file (`/var/storage/sqlite.db`) |
