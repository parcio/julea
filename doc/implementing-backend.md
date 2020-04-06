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

JULEA uses the [Meson](https://mesonbuild.com/) build system.
In case the new backend depends on additional libraries, these dependencies have to be checked first.
Specifically, the dependency's existence is checked using `pkg-config`:

```python
# Ubuntu 18.04 has LevelDB 1.20
leveldb_version = '1.20'

leveldb_dep = dependency('leveldb',
	version: '>= @0@'.format(leveldb_version),
	required: false,
)
```

Last, the backend has to be added to the list of backend targets.

```python
...

if leveldb_dep.found()
	julea_backends += 'kv/leveldb'
endif

...

foreach backend: julea_backends
	...

	if backend == 'kv/leveldb'
		extra_deps += leveldb_dep

	...
```
