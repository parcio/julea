# Debugging

There are several ways to debug JULEA.
Moreover, developers should be aware of the fact that debug builds of JULEA can behave differently than release builds:
1. The `JULEA_BACKEND_PATH` environment variable can be used to override JULEA's internal backend path only in debug builds.
2. Tracing via `JULEA_TRACE` is only available in debug builds.

## Debug Output

Debug prints from `g_debug()` can be enabled by setting the environment variable `G_MESSAGES_DEBUG` to `JULEA`.

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

## Coverage

Generating a coverage report requires the `gcovr` tool to be installed.
If it is not available on your system, you can install it as an [additional dependency](dependencies.md#additional-dependencies).
The following steps set up everything to generate a coverage report:

```console
$ . scripts/environment.sh
$ spack install py-gcovr
$ spack load py-gcovr
$ meson setup -Db_coverage=true bld
$ ninja -C bld
$ ./scripts/setup.sh start
$ ./scripts/test.sh
$ ./scripts/setup.sh stop
$ ninja -C bld coverage
```

## Tests

By default, tests that are marked as expected fail will not cause the test suite to fail as a whole.
While debugging, it sometimes makes sense to disable this behavior, which can be achieved by setting the `JULEA_TEST_FATAL_EXPECTED_FAIL` environment variable.
