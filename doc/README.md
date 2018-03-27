# README

*JULEA* is a flexible storage framework that allows offering arbitrary client interfaces to applications. To be able to rapidly prototype new approaches, it offers object and key-value backends that can either be client-side or server-side; backends for popular storage technologies such as **POSIX**, **LevelDB** and **MongoDB** have already been implemented.

Additionally, *JULEA* allows dynamically adapting the I/O operations' semantics and can thus be adjusted to different use-cases. It runs completely in user space, which eases development and debugging. Its goal is to provide a solid foundation for storage research and teaching.


## Links

* [Quick Start](Quick%20Start.md)
* [Configuration](Configuration.md)
* [Dependencies](Dependencies.md)
* [Howto implement a backend](Howto%20implement%20a%20backend.md)
