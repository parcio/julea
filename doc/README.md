# Documentation

JULEA is a flexible storage framework that allows offering arbitrary client interfaces to applications.
To be able to rapidly prototype new approaches, it offers object and key-value backends that can either be client-side or server-side;
backends for popular storage technologies such as POSIX, LevelDB and MongoDB have already been implemented.

Additionally, JULEA allows dynamically adapting the I/O operations' semantics and can thus be adjusted to different use-cases.
It runs completely in user space, which eases development and debugging.
Its goal is to provide a solid foundation for storage research and teaching.

## Links

* [Quick Start](../README.md#quick-start)
* [Dependencies](dependencies.md)
* [Installation and Usage](installation-usage.md)
* [Configuration](configuration.md)
* [Debugging](debugging.md)
* [Implementing a Backend](implementing-backend.md)
* [HDF5 Support](hdf5.md)
* [Container Support](container.md)
