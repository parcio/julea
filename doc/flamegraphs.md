# Using perf and FlameGraphs to profile JULEA

This is a short guide on how to use `perf` and FlameGraphs to analyse the performance of JULEA's server.

## Prerequisites

- `perf` is installed
- necessary permissions are set (`sudo sysctl kernel.perf_event_paranoid=0`, see [Kernel Documentation](https://www.kernel.org/doc/html/latest/admin-guide/perf-security.html))
- [FlameGraph Repository](https://github.com/brendangregg/FlameGraph) is cloned
- JULEA's development environment is loaded

## Profiling

### Record stack traces

Stack traces are recorded using perf as shown below.

```bash
perf record --call-graph dwarf -F 200 julea-server
# start desired workload (e.g., JULEA's benchmarks)
# ...
# server can be terminated using Ctrl+C
```

A few points to note:
- `-F` sets the sampling frequency in Hertz.
You may want to specify a higher value depending on the needed precision.
- Because traces can grow quite fast in size it might be usefull to get an overview first and use a higher resolution to examine only specific performance issues if possible.
- JULEA's tests are not suited for profiling because of too few operations performed.
If no specific application is given JULEA's benchmarks are suitable as profiling workload.
- Adding `-a` to the perf record call will take samples from all running applications.
This might be useful to also profile the JULEA client application if running on the same machine.

### Fold stack traces

If you want to use Brandan Gregg's FlameGraph script you need to fold the generated stack traces.
Though Speedscope, theoretically, does  not require this step it is benefical for the performance of the tool.
Since Linux 4.5, perf can generate folded stack traces.
However the script from Brendan Gregg produces better color mappings in Speedscope.

```bash
perf script | ./stackcollapse-perf.pl > out.perf-folded
```

```bash
perf report --stdio --no-children -n -g folded,0,caller,count -s comm | \
awk '/^ / { comm = $3 } /^[0-9]/ { print comm ";" $2, $1 }' > out.perf-folded
```

### Visualize

The folded stack traces can be visualized using Brendan Gregg's original FlameGraphs:

```bash
./flamegraph.pl out.perf-folded > perf.svg
firefox perf.svg  # or chrome, etc.
```

Alternatively [Speedscope](https://www.speedscope.app/) is a nice web app to interactively explore the data.

## Further Reading

- [Step by step guide from Brendan Gregg](https://www.brendangregg.com/FlameGraphs/cpuflamegraphs.html)
- [FlameGraph Repository](https://github.com/brendangregg/FlameGraph)
- [Speedscope](https://www.speedscope.app/)
- [perf record manpage](https://www.man7.org/linux/man-pages/man1/perf-record.1.html)
