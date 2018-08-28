# Implementing a Backend

Implementing an additional JULEA backend is done by providing its implementation within the `backend` directory.
Depending on the type of backend, it has to be placed in the `object` or `kv` subdirectory.

The `null` object and key-value backends can serve as starting points for a new backend.

The following headers should be included by all backends:

```c
#include <julea-config.h>

#include <glib.h>
#include <gmodule.h>

#include <julea.h>
```

The headers are followed by the actual implementation of the backend, which is divided into multiple functions.
Among others, the backend can define initialization and finalization functions:

```c
static
gboolean
backend_init (gchar const* path)
{
    (void)path;

    return TRUE;
}

static
void
backend_fini (void)
{
}
```

Finally, a `JBackend` structure has to be defined and returned to make the backend known to JULEA.

```c
static
JBackend null_backend = {
    .type = J_BACKEND_TYPE_OBJECT,
    .component = J_BACKEND_COMPONENT_CLIENT | J_BACKEND_COMPONENT_SERVER,
    .object = {
        .backend_init = backend_init,
        .backend_fini = backend_fini,
        .backend_create = backend_create,
        .backend_delete = backend_delete,
        .backend_open = backend_open,
        .backend_close = backend_close,
        .backend_status = backend_status,
        .backend_sync = backend_sync,
        .backend_read = backend_read,
        .backend_write = backend_write
    }
};

G_MODULE_EXPORT
JBackend*
backend_info (void)
{
    return &null_backend;
}
```

## Build System

JULEA uses the [Waf](https://waf.io/) build system and its build scripts are therefore written in Python.

In case the new backend depends on additional libraries, these dependencies have to be checked first.
Specifically, a new parameter has to be added for each dependency:

```python
ctx.add_option('--leveldb', action='store', default=None, help='LevelDB prefix')
```

The next step is to check for the dependency's existence using `pkg-config`.
JULEA provides `check_cfg_rpath`, which is a thin wrapper around Waf's `check_cfg` to set the `rpath`:

```python
ctx.env.JULEA_LEVELDB = \
check_cfg_rpath(
    ctx,
    package = 'leveldb',
    args = ['--cflags', '--libs'],
    uselib_store = 'LEVELDB',
    pkg_config_path = get_pkg_config_path(ctx.options.leveldb),
    mandatory = False
)
```

Last, the backend has to be added to the list of backend targets.

```python
...

if ctx.env.JULEA_LEVELDB:
    kv_backends.append('leveldb')

...

for backend in kv_backends:
    if backend == 'leveldb':
        use_extra = ['LEVELDB']

    ...
```
