# Python Bindings

The JULEA client python bindings are using cffi created bindings.
For that the spack package `py-cffi` must be installed and loaded.

```sh
. ./scripts/environment.sh
ninja -C bld # ensure binaries are up to date
spack install py-cffi # if not already installed
spack load py-cffi
python python/build.py bld # bld... your building directory
```

This will create a python library with c bindings resident in the build directory.
After this you can import the library with `import julea` as long as the environment and `py-cffi` is loaded.
A usage example can be found in `python/example/hello-world.py`

The JULEA module contains the following parts:

* `julea.ffi`: basic c defines and functions. Used for null value (`julea.ffi.NULL`) and to allocate heap variables.
   (`p = julea.ffi.new(guint*)` will allocate `guint` and return a pointer to it)
* `julea.lib`: The JULEA API, for further information please read the [API-documentation](https://julea-io.github.io/julea/). (`julea.lib.j_object_new(julea.encode('hello'), julea.encode('world'))`)
* `encode(srt) -> c_str, read_string(c_str) -> str`: string helper to convert python to c strings and the other way around.
* `JBatch` and `JBatchResult`: JULEA works in batches, therefore commands will be queued and then executed at once.
   ```python
   result = JBatchResult()
   with JBatch(result) as batch:
      lib.j_object_create(..., batch)
      ...
   if result.IsSuccess:
      ...
   ```
