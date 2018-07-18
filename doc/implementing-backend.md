# How to implement a backend

Implementing an additional backend storage system is usually done in a single `.c`-file. At the beginning, a final decision has to been taken: is this backend connectable via *JULEA* client or server?

Therefore, a file should be created in `backend/client/` or `backend/server/`.

Here is a basic template, which could be used to create an implementation:

``` {.c}
#include <julea-config.h>
#include <glib.h>
#include <gmodule.h>
#include <julea.h>

static gboolean backend_create (gchar const* namespace, gchar const* path, gpointer* data)
{
    // PREPARE STUFF
    j_trace_file_begin(full_path, J_TRACE_FILE_CREATE);
    // DO STUFF
    j_trace_file_end(full_path, J_TRACE_FILE_CREATE, 0, 0);
    // FOLLOW UP STUFF

    return TRUE;
}

static gboolean backend_open (gchar const* namespace, gchar const* path, gpointer* data)
{
    // PREPARE STUFF
    j_trace_file_begin(full_path, J_TRACE_FILE_OPEN);
    // DO STUFF
    j_trace_file_end(full_path, J_TRACE_FILE_OPEN, 0, 0);
    // FOLLOW UP STUFF

    return TRUE;
}

static gboolean backend_delete (gpointer data)
{
    // PREPARE STUFF
    j_trace_file_begin(bf->path, J_TRACE_FILE_DELETE);
    // DO STUFF
    j_trace_file_end(bf->path, J_TRACE_FILE_DELETE, 0, 0);
    // FOLLOW UP STUFF

    return TRUE;
}

static gboolean backend_close (gpointer data)
{
    // PREPARE STUFF
    j_trace_file_begin(bf->path, J_TRACE_FILE_CLOSE);
    // DO STUFF
    j_trace_file_end(bf->path, J_TRACE_FILE_CLOSE, 0, 0);
    // FOLLOW UP STUFF

    return TRUE;
}

static gboolean backend_status (gpointer data, gint64* modification_time, guint64* size)
{
    // PREPARE STUFF
    j_trace_file_begin(bf->path, J_TRACE_FILE_STATUS);
    // DO STUFF
    j_trace_file_end(bf->path, J_TRACE_FILE_STATUS, 0, 0);
    // FOLLOW UP STUFF

    return TRUE;
}

static gboolean backend_sync (gpointer data)
{
    // PREPARE STUFF
    j_trace_file_begin(bf->path, J_TRACE_FILE_SYNC);
    // DO STUFF
    j_trace_file_end(bf->path, J_TRACE_FILE_SYNC, 0, 0);
    // FOLLOW UP STUFF

    return TRUE;
}

static gboolean backend_read (gpointer data, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
    // PREPARE STUFF
    j_trace_file_begin(bf->path, J_TRACE_FILE_READ);
    // DO STUFF
    j_trace_file_end(bf->path, J_TRACE_FILE_READ, length, offset);
    // FOLLOW UP STUFF

    return TRUE;
}

static gboolean backend_write (gpointer data, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
    // PREPARE STUFF
    j_trace_file_begin(bf->path, J_TRACE_FILE_WRITE);
    // DO STUFF
    j_trace_file_end(bf->path, J_TRACE_FILE_WRITE, length, offset);
    // FOLLOW UP STUFF

    return TRUE;
}

static gboolean backend_init (gchar const* path)
{
    // DO STUFF

    return TRUE;
}

static void backend_fini (void)
{
    // DO STUFF
}

static
JBackend my_new_backend = {
    .type = J_BACKEND_TYPE_OBJECT,
    .object = {
        .init = backend_init,
        .fini = backend_fini,
        .create = backend_create,
        .delete = backend_delete,
        .open = backend_open,
        .close = backend_close,
        .status = backend_status,
        .sync = backend_sync,
        .read = backend_read,
        .write = backend_write
    }
};

G_MODULE_EXPORT
JBackend*
backend_info (JBackendType type)
{
    JBackend* backend = NULL;

    if (type == J_BACKEND_TYPE_OBJECT)
    {
        backend = &my_new_backend;
    }

    return backend;
}
```


# Add to buildscript

Currently, the Python-written buildsystem called [[Waf|https://github.com/waf-project/waf]] is been used. To add your implementation to the build chain, a little Python has to been written. The following code snippets shows, how `librados` is added to *Waf*.

At first, you want to add an optional parameter for using your new backend.

``` {.Python}
ctx.add_option('--librados', action='store', default=None, help='librados driver prefix')
```

The next step is defining an environment variable with its preferences, like libraries to use or its obligation.

``` {.python}
ctx.env.JULEA_LIBRADOS = \
ctx.check_cc(
    lib = 'rados',
    uselib_store = 'LIBRADOS',
    mandatory = False
)
```

At least, tell *Waf* weather you backend is a client or server component.

``` {.python}
if ctx.env.JULEA_LIBRADOS:
    backends_client.append('rados')

for backend in backends_client:
    use_extra = []

    if backend == 'mongodb':
        use_extra = ['LIBMONGOC']
    elif backend == 'rados':
       use_extra = ['LIBRADOS']
```
