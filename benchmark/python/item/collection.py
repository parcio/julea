from benchmarkrun import BenchmarkRun, append_to_benchmark_list_and_run
from julea import lib, encode, ffi

def benchmark_collection(benchmarkrun_list, iterations, machine_readable):
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/collection/create", iterations, machine_readable), benchmark_collection_create)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/collection/create-batch", iterations, machine_readable), benchmark_collection_create_batch)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/collection/delete", iterations, machine_readable), benchmark_collection_delete)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/collection/delete-batch", iterations, machine_readable), benchmark_collection_delete_batch)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/collection/delete-batch-without-get", iterations, machine_readable), benchmark_collection_delete_batch_without_get)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/collection/unordered-create-delete", iterations, machine_readable), benchmark_collection_unordered_create_delete)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/collection/unordered-create-delete-batch", iterations, machine_readable), benchmark_collection_unordered_create_delete_batch)
    # TODO: benchmark get (also missing in c benchmark)

def benchmark_collection_create(run):
    _benchmark_collection_create(run, False)

def benchmark_collection_create_batch(run):
    _benchmark_collection_create(run, True)

def _benchmark_collection_create(run, use_batch):
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    delete_batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    run.start_timer()
    for i in range(run.iterations):
        name = encode(f"benchmark{i}")
        collection = lib.j_collection_create(name, batch)
        lib.j_collection_delete(collection, delete_batch)
        if not use_batch:
            assert lib.j_batch_execute(batch)
        lib.j_collection_unref(collection)
    if use_batch:
        assert lib.j_batch_execute(batch)
    run.stop_timer()
    assert lib.j_batch_execute(delete_batch)
    lib.j_batch_unref(batch)
    lib.j_batch_unref(delete_batch)

def benchmark_collection_delete(run):
    _benchmark_collection_delete(run, False)

def benchmark_collection_delete_batch(run):
    _benchmark_collection_delete(run, True)

def _benchmark_collection_delete(run, use_batch):
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    for i in range(run.iterations):
        name = encode(f"benchmark-{i}")
        collection = lib.j_collection_create(name, batch)
        lib.j_collection_unref(collection)
    assert lib.j_batch_execute(batch)
    run.start_timer()
    for i in range(run.iterations):
        collection_ptr = ffi.new("JCollection**")
        name = encode(f"benchmark-{i}")
        lib.j_collection_get(collection_ptr, name, batch)
        assert lib.j_batch_execute(batch)
        lib.j_collection_delete(collection_ptr[0], batch)
        if not use_batch:
            assert lib.j_batch_execute(batch)
    if use_batch:
        assert lib.j_batch_execute(batch)
    run.stop_timer()
    lib.j_batch_unref(batch)

def benchmark_collection_delete_batch_without_get(run):
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    delete_batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    for i in range(run.iterations):
        name = encode(f"benchmark-{i}")
        collection = lib.j_collection_create(name, batch)
        lib.j_collection_delete(collection, delete_batch)
        lib.j_collection_unref(collection)
    assert lib.j_batch_execute(batch)
    run.start_timer()
    assert lib.j_batch_execute(delete_batch)
    run.stop_timer()
    lib.j_batch_unref(batch)
    lib.j_batch_unref(delete_batch)

def benchmark_collection_unordered_create_delete(run):
    _benchmark_collection_unordered_create_delete(run, False)

def benchmark_collection_unordered_create_delete_batch(run):
    _benchmark_collection_unordered_create_delete(run, True)

def _benchmark_collection_unordered_create_delete(run, use_batch):
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    run.start_timer()
    for i in range(run.iterations):
        name = encode(f"benchmark-{i}")
        collection = lib.j_collection_create(name, batch)
        lib.j_collection_delete(collection, batch)
        if not use_batch:
            assert lib.j_batch_execute(batch)
        lib.j_collection_unref(collection)
    if use_batch:
        assert lib.j_batch_execute(batch)
    run.stop_timer()
    run.operations = run.iterations * 2
    lib.j_batch_unref(batch)
