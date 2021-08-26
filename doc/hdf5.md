# HDF5 Support

JULEA supports HDF5 applications via two Virtual Object Layer (VOL) plugins:
1. The `julea-kv` VOL plugin stores data as distributed objects using the object client and metadata as key-value pairs using the kv client.
2. The `julea-db` VOL plugin stores data as distributed objects using the object client and metadata as database entries using the db client.

To make use of JULEA's HDF5 support, make sure that you have set up JULEA using either the [Quick Start](../README.md#quick-start) or the [Installation and Usage](installation-usage.md) documentation.

JULEA's environment script will set `HDF5_PLUGIN_PATH`, which allows HDF5 to find JULEA's VOL plugins.
The VOL plugin to use can be selected using the `HDF5_VOL_CONNECTOR` environment variable:

```console
$ . scripts/environment.sh
$ export HDF5_VOL_CONNECTOR=julea-kv
$ my-application
```

## Example: Enzo

The be able to test JULEA's HDF5 plugins using real applications, [Enzo](https://enzo-project.org/) can be used.
The following steps install Enzo via Spack and run one of its examples using JULEA's `julea-db` VOL plugin.

```console
$ . scripts/environment.sh
$ spack install enzo@master ^hdf5@1.12:
$ spack load enzo
$ export HDF5_VOL_CONNECTOR=julea-db
$ ./scripts/setup.sh start
# It might currently be necessary to start Enzo twice due to a bug
$ enzo -d "$(spack location -i enzo)/run/Hydro/Hydro-3D/CollapseTestNonCosmological/CollapseTestNonCosmological.enzo"
$ ./scripts/setup.sh stop
```
