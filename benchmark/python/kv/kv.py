from benchmarkrun import BenchmarkRun, append_to_benchmark_list_and_run
from julea import lib, encode, ffi

def benchmark_kv(benchmarkrun_list, iterations, machine_readable):
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/kv/put", iterations, machine_readable), benchmark_kv_put)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/kv/put_batch", iterations, machine_readable), benchmark_kv_put_batch)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/kv/get", iterations, machine_readable), benchmark_kv_get)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/kv/get-batch", iterations, machine_readable), benchmark_kv_get_batch)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/kv/delete", iterations, machine_readable), benchmark_kv_delete)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/kv/delete-batch", iterations, machine_readable), benchmark_kv_delete_batch)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/kv/unordered_put_delete", iterations, machine_readable), benchmark_kv_unordered_put_delete)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/kv/unordered_put_delete_batch", iterations, machine_readable), benchmark_kv_unordered_put_delete_batch)

def benchmark_kv_put(run):
    _benchmark_kv_put(run, False)

def benchmark_kv_put_batch(run):
    _benchmark_kv_put(run, True)

def _benchmark_kv_put(run, use_batch):
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    deletebatch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    run.start_timer()
    for i in range(run.iterations):
        name = encode(f"benchmark-{i}")
        namespace = encode("benchmark")
        kv = lib.j_kv_new(namespace, name)
        empty = encode("empty")
        lib.j_kv_put(kv, empty, 6, ffi.NULL, batch)
        lib.j_kv_delete(kv, deletebatch)
        if not use_batch:
            assert lib.j_batch_execute(batch)
        lib.j_kv_unref(kv)
    if use_batch:
        assert lib.j_batch_execute(batch)
    run.stop_timer()
    assert lib.j_batch_execute(deletebatch)
    lib.j_batch_unref(batch)
    lib.j_batch_unref(deletebatch)

def benchmark_kv_get(run):
    _benchmark_kv_get(run, False)

def benchmark_kv_get_batch(run):
    _benchmark_kv_get(run, True)

@ffi.def_extern()
def cffi_j_kv_get_function(pointer1, integer, pointer2):
    return

def _benchmark_kv_get(run, use_batch):
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    deletebatch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    for i in range(run.iterations):
        name = encode(f"benchmark-{i}")
        namespace = encode("benchmark")
        kv = lib.j_kv_new(namespace, name)
        lib.j_kv_put(kv, name, len(name), ffi.NULL, batch)
        lib.j_kv_delete(kv, deletebatch)
        lib.j_kv_unref(kv)
    assert lib.j_batch_execute(batch)
    run.start_timer()
    for i in range(run.iterations):
        name = encode(f"benchmark-{i}")
        namespace = encode("benchmark")
        kv = lib.j_kv_new(namespace, name)
        lib.j_kv_get_callback(kv, lib.cffi_j_kv_get_function, ffi.NULL, batch)
        if not use_batch:
            assert lib.j_batch_execute(batch)
        lib.j_kv_unref(kv)
    if use_batch:
        assert lib.j_batch_execute(batch)
    run.stop_timer()
    assert lib.j_batch_execute(deletebatch)
    lib.j_batch_unref(batch)
    lib.j_batch_unref(deletebatch)

def benchmark_kv_delete(run):
    _benchmark_kv_delete(run, False)

def benchmark_kv_delete_batch(run):
    _benchmark_kv_delete(run, True)

def _benchmark_kv_delete(run, use_batch):
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    for i in range(run.iterations):
        name = encode(f"benchmark-{i}")
        namespace = encode("benchmark")
        kv = lib.j_kv_new(namespace, name)
        empty = encode("empty")
        lib.j_kv_put(kv, empty, 6, ffi.NULL, batch)
        lib.j_kv_unref(kv)
    assert lib.j_batch_execute(batch)
    run.start_timer()
    for i in range(run.iterations):
        name = encode(f"benchmark-{i}")
        namespace = encode("benchmark")
        kv = lib.j_kv_new(namespace, name)
        lib.j_kv_delete(kv, batch)
        if not use_batch:
            assert lib.j_batch_execute(batch)
        lib.j_kv_unref(kv)
    if use_batch:
        assert lib.j_batch_execute(batch)
    run.stop_timer()
    lib.j_batch_unref(batch)

def benchmark_kv_unordered_put_delete(run):
    _benchmark_kv_unordered_put_delete(run, False)

def benchmark_kv_unordered_put_delete_batch(run):
    _benchmark_kv_unordered_put_delete(run, True)

def _benchmark_kv_unordered_put_delete(run, use_batch):
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    run.start_timer()
    for i in range(run.iterations):
        name = encode(f"benchmark-{i}")
        namespace = encode("benchmark")
        kv = lib.j_kv_new(namespace, name)
        empty = encode("empty")
        lib.j_kv_put(kv, empty, 6, ffi.NULL, batch)
        lib.j_kv_delete(kv, batch)
        if not use_batch:
            assert lib.j_batch_execute(batch)
        lib.j_kv_unref(kv)
    if use_batch:
        assert lib.j_batch_execute(batch)
    run.stop_timer()
    lib.j_batch_unref(batch)
    run.operations = run.iterations * 2
