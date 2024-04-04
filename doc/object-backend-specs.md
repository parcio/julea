
# Object Backend Specification

This specification aims to help with implementing new backends by  defining what JULEA expects a backend to do when it calls one of its functions.

> [!NOTE]
> Unfortunately, the current test suite do not cover every aspect of the specification nor does every violation of the specification immediately lead to unexpected behavior. Thus, for the time being, special attention and care on the side of the backend implementor is required.

### Error Handling

By default, the backend's functions are expected to return `TRUE`.
If, for whatever reason, the backend cannot perform a task following the specifications below, it must fail by returning `FALSE`.
Additional failure conditions are outlined in the sections below.

### backend_init

JULEA calls this function to allow the backend to initialize itself under the given path. The path consists of an arbitrary number of directories. It's the backend's responsibility to create any missing directories.

**Parameters**
- `path`: The path under which to initialize the backend.
- `backend_data`: Output parameter for the backend.

### backend_fini

JULEA calls this function to allow the backend to perform clean-up before shutdown.

**Parameters**
- `backend_data`: The backend JULEA is about to destroy.

**Postcondtions**
- the backend does not have any I/O operations in flight
- all memory allocated by the backend is freed

### backend_create

This function creates a new object specified by its full path.

Both the namespace and the path can contain an arbitrary number of parent directories. It's the responsibility of the backend to create any missing parent directories.

**Parameters**
- `backend_data`: The backend in which to create the object.
- `namespace`: The namespace used to identify the created object.
- `path`: The path used to identify the created object.
- `backend_object`: Output parameter for the created object.

**Failure Conditions**
- the full path resolves to an existing object

### backend_open

This function opens an existing object specified by its full path.

Both the namespace and the path can contain an arbitrary number of parent directories. It's the responsibility of the backend to create any missing parent directories.

**Parameters**
- `backend_data`: The backend in which to open the object.
- `namespace`: The namespace used to identify the opened object.
- `path`: The path used to identify the opened object.
- `backend_object`: Output parameter for the opened object.

**Failure Conditions**
- the full path does not resolve to an existing object

**Output**
- a reference to an object handle must be stored in `backend_object`.

### backend_delete

This function deletes an existing object, specified by its handle.

**Parameters**
- `backend_data`: The backend containing the object.
- `backend_object`: The object that is to be deleted.

**Postconditions**
- the object handle no longer resolves to a valid object
- the object cannot be reloaded from persistent storage

**Failure Conditions**
- the object handle does not resolve to an object

### backend_close

This function closes an existing object, specified by its handle.

**Parameters**
- `backend_data`: The backend containing the object.
- `backend_object`: The object that is to be closed.

**Postconditions**
- the object handle no longer resolves to a valid object

**Failure Conditions**
- the object handle does not resolve to an object
- the object was already closed

### backend_read

This function reads a specified number of bytes at a specified offset from an object specified by its handle into a buffer. Short reads are allowed.

**Parameters**
- `backend_data`: The backend containing the object.
- `backend_object`: The object to read from.
- `buffer`: Output parameter to write the content of the object into.
- `length`: The number of bytes to read.
- `offset`: The offset at which to read from the object.
- `bytes_read`: Output parameter for the number of bytes that have been read from the object.

**Preconditions**
- the buffer's size is at least `length`

**Failure Conditions**
- the object handle does not resolve to an object.
- read stops early due to an error (to distinguish from a valid short read)

### backend_write

This function writes a specified number of bytes from a buffer into an object specified by its handle at the specified offset. The object must be appended, if necessary.

- `backend_data`: The backend containing the object.
- `backend_object`: The object to write to.
- `buffer`: The buffer to write to the object.
- `length`: The number of bytes to write.
- `offset`: The offset at which to write to the object.
- `bytes_written`: Output parameter for the number of bytes that have been written to the object.

**Preconditions**
- the buffer's size is at least `length`

**Failure Conditions**
- the object handle does not resolve to an object
- unable to write entire buffer to object (for example because the system ran out of storage space)

### backend_status

This function fetches metadata from an object specified by its handle.

**Parameters**
- `backend_data`: The backend containing the object.
- `backend_object`: The object to acquire metadata from.
- `modification_time`: Output parameter for the time of the object's most recent modification.
- `size`: Output parameter for the object's size in bytes.

**Failure Conditions**
- the object handle does not resolve to an object

### backend_sync

This function flushes an object specified by its handle.

**Parameters**
- `backend_data`: The backend containing the object.
- `backend_object`: The object to synchronize.

**Post Conditions**
- no I/O operations on the specified object are in flight
- the object's state matches the state in persistent storage

**Failure Conditions**
- the object handle does not resolve to an object

### backend_get_all

Creates a new iterator that iterates over all objects under a specified path. The path is defined as `${backend_data.path}/${namespace}`.

**Parameters**
- `backend_data`: The backend containing on which to iterate.
- `backend_iterator`: Output parameter for the newly created iterator.

**Failure Conditions**
- the specified path does not resolve to an existing directory

### backend_get_by_prefix

Creates a new iterator that iterates over all objects under a specified path matching a specified prefix. The path is defined as `${backend_data.path}/${namespace}`.

**Parameters**
- `backend_data`: The backend containing a directory on which to iterate.
- `backend_iterator`: Output parameter for the newly created iterator.

**Failure Conditions**
- the specified path does not resolve to an existing directory

### backend_iterate

This function advances the iterator to the next object. If the end of the iterator is reached the iterator must be freed.

**Parameters**
- `backend_data`: The backend containing a directory on which to iterate.
- `backend_iterator`: The backend iterator to advance to the next entry.
- `name`: Output parameter for the file the iterator advanced to.

**Failure Conditions**
- reached the end of the iterator
