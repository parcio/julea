from benchmarkrun import BenchmarkRun, append_to_benchmark_list_and_run
from julea import lib, encode, ffi

def benchmark_item(benchmarkrun_list, iterations, machine_readable):
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/item/create", iterations, machine_readable), benchmark_item_create)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/item/create-batch", iterations, machine_readable), benchmark_item_create_batch)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/item/delete", iterations, machine_readable), benchmark_item_delete)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/item/delete-batch", iterations, machine_readable), benchmark_item_delete_batch)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/item/delete-batch-without-get", iterations, machine_readable), benchmark_item_delete_batch_without_get)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/item/get-status", iterations, machine_readable), benchmark_item_get_status)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/item/get-status-batch", iterations, machine_readable), benchmark_item_get_status_batch)
    # TODO: benchmark get (also missing in c benchmark)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/item/read", iterations, machine_readable), benchmark_item_read)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/item/read-batch", iterations, machine_readable), benchmark_item_read_batch)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/item/write", iterations, machine_readable), benchmark_item_write)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/item/write-batch", iterations, machine_readable), benchmark_item_write_batch)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/item/unordered-create-delete", iterations, machine_readable), benchmark_item_unordered_create_delete)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/item/item/unordered-create-delete-batch", iterations, machine_readable), benchmark_item_unordered_create_delete_batch)

def benchmark_item_create(run):
    _benchmark_item_create(run, False)

def benchmark_item_create_batch(run):
    _benchmark_item_create(run, True)

def _benchmark_item_create(run, use_batch):
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    delete_batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    collection_name = encode("benchmark")
    collection = lib.j_collection_create(collection_name, batch)
    assert lib.j_batch_execute(batch)
    run.start_timer()
    for i in range(run.iterations):
        name = encode(f"benchmark-{i}")
        item = lib.j_item_create(collection, name, ffi.NULL, batch)
        lib.j_item_delete(item, delete_batch)
        if not use_batch:
            assert lib.j_batch_execute(batch)
        lib.j_item_unref(item)
    if use_batch:
        assert lib.j_batch_execute(batch)
    run.stop_timer()
    assert lib.j_batch_execute(delete_batch)
    lib.j_collection_unref(collection)
    lib.j_batch_unref(batch)
    lib.j_batch_unref(delete_batch)

def benchmark_item_delete(run):
    _benchmark_item_delete(run, False)

def benchmark_item_delete_batch(run):
    _benchmark_item_delete(run, True)

def _benchmark_item_delete(run, use_batch):
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    get_batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    collection_name = encode("benchmark")
    collection = lib.j_collection_create(collection_name, batch)
    assert lib.j_batch_execute(batch)
    for i in range(run.iterations):
        name = encode(f"benchmark-{i}")
        item = lib.j_item_create(collection, name, ffi.NULL, batch)
        lib.j_item_unref(item)
    assert lib.j_batch_execute(batch)
    run.start_timer()
    for i in range(run.iterations):
        ptr = ffi.new("int**")
        item_ptr = ffi.cast("JItem**", ptr)
        name = encode(f"benchmark-{i}")
        lib.j_item_get(collection, item_ptr, name, get_batch)
        assert lib.j_batch_execute(get_batch)
        lib.j_item_delete(item_ptr[0], batch)
        if not use_batch:
            assert lib.j_batch_execute(batch)
    if use_batch:
        assert lib.j_batch_execute(batch)
    run.stop_timer()
    lib.j_collection_delete(collection, batch)
    assert lib.j_batch_execute(batch)
    lib.j_collection_unref(collection)
    lib.j_batch_unref(batch)
    lib.j_batch_unref(get_batch)

def benchmark_item_delete_batch_without_get(run):
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    delete_batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    collection_name = encode("benchmark")
    collection = lib.j_collection_create(collection_name, batch)
    assert lib.j_batch_execute(batch)
    for i in range(run.iterations):
        name = encode("benchmark-{i}")
        item = lib.j_item_create(collection, name, ffi.NULL, batch)
        lib.j_item_delete(item, delete_batch)
        lib.j_item_unref(item)
    assert lib.j_batch_execute(batch)
    run.start_timer()
    assert lib.j_batch_execute(delete_batch)
    run.stop_timer()
    lib.j_collection_delete(collection, batch)
    lib.j_collection_unref(collection)
    lib.j_batch_unref(batch)
    lib.j_batch_unref(delete_batch)

def benchmark_item_get_status(run):
    _benchmark_item_get_status(run, False)

def benchmark_item_get_status_batch(run):
    _benchmark_item_get_status(run, True)

def _benchmark_item_get_status(run, use_batch):
    run.iterations = 10 * run.iterations if use_batch else run.iterations
    run.operations = run.iterations
    dummy = ffi.new("char[1]")
    nb_ptr = ffi.new("unsigned long*")
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    collection_name = encode("benchmark")
    collection = lib.j_collection_create(collection_name, batch)
    item = lib.j_item_create(collection, collection_name, ffi.NULL, batch)
    lib.j_item_write(item, dummy, 1, 0, nb_ptr, batch)
    assert lib.j_batch_execute(batch)
    run.start_timer()
    for i in range(run.iterations):
        lib.j_item_get_status(item, batch)
        if not use_batch:
            assert lib.j_batch_execute(batch)
    if use_batch:
        assert lib.j_batch_execute(batch)
    run.stop_timer()
    lib.j_item_delete(item, batch)
    lib.j_collection_delete(collection, batch)
    assert lib.j_batch_execute(batch)
    lib.j_item_unref(item)
    lib.j_collection_unref(collection)
    lib.j_batch_unref(batch)

def benchmark_item_read(run):
    _benchmark_item_read(run, False, 4 * 1024)

def benchmark_item_read_batch(run):
    _benchmark_item_read(run, True, 4 * 1024)

def _benchmark_item_read(run, use_batch, block_size):
    run.iterations = 10 * run.iterations if use_batch else run.iterations
    run.operations = run.iterations
    nb_ptr = ffi.new("unsigned long*")
    dummy = ffi.new("char[]", block_size)
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    collection_name = encode("benchmark")
    collection = lib.j_collection_create(collection_name, batch)
    item = lib.j_item_create(collection, collection_name, ffi.NULL, batch)
    for i in range(run.iterations):
        lib.j_item_write(item, dummy, block_size, i * block_size, nb_ptr, batch)
    assert lib.j_batch_execute(batch)
    assert nb_ptr[0] == run.iterations * block_size
    run.start_timer()
    for i in range(run.iterations):
        lib.j_item_read(item, dummy, block_size, i * block_size, nb_ptr, batch)
        if not use_batch:
            assert lib.j_batch_execute(batch)
            assert nb_ptr[0] == block_size
    if use_batch:
        assert lib.j_batch_execute(batch)
        assert nb_ptr[0] == run.iterations * block_size
    run.stop_timer()
    lib.j_item_delete(item, batch)
    lib.j_collection_delete(collection, batch)
    assert lib.j_batch_execute(batch)
    lib.j_item_unref(item)
    lib.j_collection_unref(collection)
    lib.j_batch_unref(batch)

def benchmark_item_write(run):
    _benchmark_item_write(run, False, 4 * 1024)

def benchmark_item_write_batch(run):
    _benchmark_item_write(run, True, 4 * 1024)

def _benchmark_item_write(run, use_batch, block_size):
    run.iterations = 10 * run.iterations if use_batch else run.iterations
    run.operations = run.iterations
    dummy = ffi.new("char[]", block_size)
    nb_ptr = ffi.new("unsigned long*")
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    collection_name = encode("benchmark")
    collection = lib.j_collection_create(collection_name, batch)
    item = lib.j_item_create(collection, collection_name, ffi.NULL, batch)
    assert lib.j_batch_execute(batch)
    run.start_timer()
    for i in range(run.iterations):
        lib.j_item_write(item, dummy, block_size, i * block_size, nb_ptr, batch)
        if not use_batch:
            assert lib.j_batch_execute(batch)
            assert nb_ptr[0] == block_size
    if use_batch:
        assert lib.j_batch_execute(batch)
        assert nb_ptr[0] == run.iterations * block_size
    run.stop_timer()
    lib.j_item_delete(item, batch)
    lib.j_collection_delete(collection, batch)
    assert lib.j_batch_execute(batch)
    lib.j_item_unref(item)
    lib.j_collection_unref(collection)
    lib.j_batch_unref(batch)

def benchmark_item_unordered_create_delete(run):
    _benchmark_item_unordered_create_delete(run, False)

def benchmark_item_unordered_create_delete_batch(run):
    _benchmark_item_unordered_create_delete(run, True)

def _benchmark_item_unordered_create_delete(run, use_batch):
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    collection_name = encode("benchmark")
    collection = lib.j_collection_create(collection_name, batch)
    assert lib.j_batch_execute(batch)
    run.start_timer()
    for i in range(run.iterations):
        name = encode(f"benchmark-{i}")
        item = lib.j_item_create(collection, name, ffi.NULL, batch)
        lib.j_item_delete(item, batch)
        if not use_batch:
            assert lib.j_batch_execute(batch)
        lib.j_item_unref(item)
    if use_batch:
        assert lib.j_batch_execute(batch)
    run.stop_timer()
    lib.j_collection_delete(collection, batch)
    assert lib.j_batch_execute(batch)
    lib.j_collection_unref(collection)
    lib.j_batch_unref(batch)
    run.operations = run.iterations * 2
