from benchmarkrun import BenchmarkRun, append_to_benchmark_list_and_run
from julea import lib, encode, ffi
from db.common import _benchmark_db_insert, _benchmark_db_prepare_scheme, _benchmark_db_get_identifier, N, N_GET_DIVIDER, N_PRIME, SIGNED_FACTOR, CLASS_MODULUS, CLASS_LIMIT

def benchmark_db_entry(benchmarkrun_list, iterations, machine_readable):
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/insert", iterations, machine_readable), benchmark_db_insert)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/insert-batch", iterations, machine_readable), benchmark_db_insert_batch)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/insert-index-single", iterations, machine_readable), benchmark_db_insert_index_single)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/insert-batch-index-single", iterations, machine_readable), benchmark_db_insert_batch_index_single)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/insert-index-all", iterations, machine_readable), benchmark_db_insert_index_all)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/insert-batch-index-all", iterations, machine_readable), benchmark_db_insert_batch_index_all)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/insert-index-mixed", iterations, machine_readable), benchmark_db_insert_index_mixed)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/insert-batch-index-mixed", iterations, machine_readable), benchmark_db_insert_batch_index_mixed)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/delete", iterations, machine_readable), benchmark_db_delete)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/delete-batch", iterations, machine_readable), benchmark_db_delete_batch)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/delete-index-single", iterations, machine_readable), benchmark_db_delete_index_single)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/delete-batch-index-single", iterations, machine_readable), benchmark_db_delete_batch_index_single)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/delete-index-all", iterations, machine_readable), benchmark_db_delete_index_all)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/delete-batch-index-all", iterations, machine_readable), benchmark_db_delete_batch_index_all)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/delete-index-mixed", iterations, machine_readable), benchmark_db_delete_index_mixed)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/delete-batch-index-mixed", iterations, machine_readable), benchmark_db_delete_batch_index_mixed)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/update", iterations, machine_readable), benchmark_db_update)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/update-batch", iterations, machine_readable), benchmark_db_update_batch)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/update-index-single", iterations, machine_readable), benchmark_db_update_index_single)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/update-batch-index-single", iterations, machine_readable), benchmark_db_update_batch_index_single)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/update-index-all", iterations, machine_readable), benchmark_db_update_index_all)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/update-batch-index-all", iterations, machine_readable), benchmark_db_update_batch_index_all)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/update-index-mixed", iterations, machine_readable), benchmark_db_update_index_mixed)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/entry/update-batch-index-mixed", iterations, machine_readable), benchmark_db_update_batch_index_mixed)

def benchmark_db_insert(run):
    _benchmark_db_insert(run, None, "benchmark_insert", False, False, False,
                         True)

def benchmark_db_insert_batch(run):
    _benchmark_db_insert(run, None, "benchmark_insert_batch", True, False,
                         False, True)

def benchmark_db_insert_index_single(run):
    _benchmark_db_insert(run, None, "benchmark_insert_index_single", False,
                         False, True, True)

def benchmark_db_insert_batch_index_single(run):
    _benchmark_db_insert(run, None, "benchmark_insert_batch_index_single", True,
                         False, True, True)

def benchmark_db_insert_index_all(run):
    _benchmark_db_insert(run, None, "benchmark_insert_index_all", False, True,
                         False, True)

def benchmark_db_insert_batch_index_all(run):
    _benchmark_db_insert(run, None, "benchmark_insert_batch_index_all", True,
                         True, False, True)

def benchmark_db_insert_index_mixed(run):
    _benchmark_db_insert(run, None, "benchmakr_insert_index_mixed", False, True,
                         True, True)

def benchmark_db_insert_batch_index_mixed(run):
    _benchmark_db_insert(run, None, "benchmakr_insert_batch_index_mixed", True,
                         True, True, True)

def benchmark_db_delete(run):
    _benchmark_db_delete(run, "benchmark_delete", False, False, False)

def benchmark_db_delete_batch(run):
    _benchmark_db_delete(run, "benchmark_delete_batch", True, False, False)

def benchmark_db_delete_index_single(run):
    _benchmark_db_delete(run, "benchmark_delete_index_single", False, False,
                         True)

def benchmark_db_delete_batch_index_single(run):
    _benchmark_db_delete(run, "benchmark_delete_batch_index_single", True,
                         False, True)

def benchmark_db_delete_index_all(run):
    _benchmark_db_delete(run, "benchmark_delete_index_all", False, True, False)

def benchmark_db_delete_batch_index_all(run):
    _benchmark_db_delete(run, "benchmark_delete_batch_index_all", True, True,
                         False)

def benchmark_db_delete_index_mixed(run):
    _benchmark_db_delete(run, "benchmark_delete_index_mixed", False, True, True)

def benchmark_db_delete_batch_index_mixed(run):
    _benchmark_db_delete(run, "benchmark_delete_batch_index_mixed", True, True,
                         True)

def _benchmark_db_delete(run, namespace, use_batch, use_index_all,
                         use_index_single):
    namespace_encoded = encode(namespace)
    b_s_error_ptr = ffi.new("GError*")
    b_s_error_ptr = ffi.NULL
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    delete_batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    b_scheme = _benchmark_db_prepare_scheme(namespace_encoded, False,
                                            use_index_all, use_index_single,
                                            batch, delete_batch)
    assert b_scheme != ffi.NULL
    assert run != None
    _benchmark_db_insert(None, b_scheme, "\0", True, False, False, False)
    run.start_timer()
    iterations = N if use_index_all or use_index_single else int(N / N_GET_DIVIDER)
    for i in range(iterations):
        entry = lib.j_db_entry_new(b_scheme, b_s_error_ptr)
        string = encode(_benchmark_db_get_identifier(i))
        string_name = encode("string")
        selector = lib.j_db_selector_new(b_scheme, lib.J_DB_SELECTOR_MODE_AND,
                                         b_s_error_ptr)
        assert lib.j_db_selector_add_field(selector, string_name,
                                           lib.J_DB_SELECTOR_OPERATOR_EQ,
                                           string, 0, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        assert lib.j_db_entry_delete(entry, selector, batch, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        if not use_batch:
            assert lib.j_batch_execute(batch)
        lib.j_db_entry_unref(entry)
        lib.j_db_selector_unref(selector)
    if use_batch:
            assert lib.j_batch_execute(batch)
    run.stop_timer()
    assert lib.j_batch_execute(delete_batch)
    run.operations = iterations
    lib.j_batch_unref(batch)
    lib.j_batch_unref(delete_batch)
    lib.j_db_schema_unref(b_scheme)

def benchmark_db_update(run):
    _benchmark_db_update(run, "benchmark_update", False, False, False)

def benchmark_db_update_batch(run):
    _benchmark_db_update(run, "benchmark_update_batch", True, False, False)

def benchmark_db_update_index_single(run):
    _benchmark_db_update(run, "benchmark_update_index_single", False, False,
                         True)

def benchmark_db_update_batch_index_single(run):
    _benchmark_db_update(run, "benchmark_update_batch_index_single", True,
                         False, True)

def benchmark_db_update_index_all(run):
    _benchmark_db_update(run, "benchmark_update_index_all", False, True, False)

def benchmark_db_update_batch_index_all(run):
    _benchmark_db_update(run, "benchmark_update_batch_index_all", True, True,
                         False)

def benchmark_db_update_index_mixed(run):
    _benchmark_db_update(run, "benchmark_update_index_mixed", False, True, True)

def benchmark_db_update_batch_index_mixed(run):
    _benchmark_db_update(run, "benchmark_update_batch_index_mixed", True, True,
                         True)

def _benchmark_db_update(run, namespace, use_batch, use_index_all,
                         use_index_single):
    namespace_encoded = encode(namespace)
    b_s_error_ptr = ffi.new("GError*")
    b_s_error_ptr = ffi.NULL
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    delete_batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    b_scheme = _benchmark_db_prepare_scheme(namespace_encoded, False,
                                            use_index_all, use_index_single,
                                            batch, delete_batch)
    assert b_scheme != ffi.NULL
    assert run != None
    _benchmark_db_insert(None, b_scheme, "\0", True, False, False, False)
    run.start_timer()
    iterations = N if use_index_all or use_index_single else int(N / N_GET_DIVIDER)
    for i in range(iterations):
        sint_name = encode("sint")
        i_signed_ptr = ffi.new("long*")
        i_signed_ptr[0] = (((i + N_PRIME) * SIGNED_FACTOR) & CLASS_MODULUS) - CLASS_LIMIT
        selector = lib.j_db_selector_new(b_scheme, lib.J_DB_SELECTOR_MODE_AND,
                                         b_s_error_ptr)
        entry = lib.j_db_entry_new(b_scheme, b_s_error_ptr)
        string_name = encode("string")
        string = encode(_benchmark_db_get_identifier(i))
        assert b_s_error_ptr == ffi.NULL
        assert lib.j_db_entry_set_field(encode, sint_name, i_signed_ptr, 0,
                                        b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        assert lib.j_db_selector_add_field(selector, string_name,
                                           lib.J_DB_SELECTOR_OPERATOR_EQ,
                                           string, 0, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        assert lib.j_db_entry_update(entry, selector, batch, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        if not use_batch:
            assert lib.j_batch_execute(batch)
        lib.j_db_selector_unref(selector)
        lib.j_db_entry_unref(entry)
    if use_batch:
        assert lib.j_batch_execute(batch)
    run.stop_timer()
    assert lib.j_batch_execute(delete_batch)
    run.operations = iterations
    lib.j_batch_unref(batch)
    lib.j_batch_unref(delete_batch)
    lib.j_db_schema_unref(b_scheme)
