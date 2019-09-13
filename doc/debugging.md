# Debugging

There are several ways to debug JULEA.
Moreover, developers should be aware of the fact that debug builds of JULEA behave differently than release builds.

## Tracing

JULEA contains a tracing component that can be used to record information about various aspects of JULEA's behavior.
The overall tracing can be influenced using the `JULEA_TRACE` environment variable.
If the variable is set to `echo`, all tracing information will be printed to stderr.
If JULEA has been built with OTF support, a value of `otf` will cause JULEA to produce traces via OTF.
It is also possible to specify multiple values separated by commas.

By default, all functions are traced.
If this produces too much output, a filter can be set using the `JULEA_TRACE_FUNCTION` environment variable.
The variable can contain a list of function wildcards that are separated by commas.
The wildcards support `*` and `?`.
