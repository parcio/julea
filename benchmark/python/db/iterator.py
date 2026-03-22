from benchmarkrun import BenchmarkRun, append_to_benchmark_list_and_run
from julea import lib, encode, ffi
from db.common import _benchmark_db_prepare_scheme, _benchmark_db_insert, _benchmark_db_get_identifier, N, N_GET_DIVIDER, CLASS_MODULUS, CLASS_LIMIT

def benchmark_db_iterator(benchmarkrun_list, iterations, machine_readable):
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/iterator/get-simple", iterations, machine_readable), benchmark_db_get_simple)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/iterator/get-simple-index-single", iterations, machine_readable), benchmark_db_get_simple_index_single)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/iterator/get-simple-index-all", iterations, machine_readable), benchmark_db_get_simple_index_all)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/iterator/get-simple-index-mixed", iterations, machine_readable), benchmark_db_get_simple_index_mixed)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/iterator/get-range", iterations, machine_readable), benchmark_db_get_range)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/iterator/get-range-index-single", iterations, machine_readable), benchmark_db_get_range_index_single)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/iterator/get-range-index-all", iterations, machine_readable), benchmark_db_get_range_index_all)
    append_to_benchmark_list_and_run(benchmarkrun_list, BenchmarkRun("/db/iterator/get-range-index-mixed", iterations, machine_readable), benchmark_db_get_range_index_mixed)

def benchmark_db_get_simple(run):
    _benchmark_db_get_simple(run, "benchmark_get_simple", False, False)

def benchmark_db_get_simple_index_single(run):
    _benchmark_db_get_simple(run, "benchmark_get_simple_index_single", False,
                             True)

def benchmark_db_get_simple_index_all(run):
    _benchmark_db_get_simple(run, "benchmark_get_simple_index_all", True, False)

def benchmark_db_get_simple_index_mixed(run):
    _benchmark_db_get_simple(run, "benchmark_get_simple_index_mixed", True, True)

def _benchmark_db_get_simple(run, namespace, use_index_all, use_index_single):
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
        field_type_ptr = ffi.new("JDBType*")
        field_value_ptr = ffi.new("void**")
        field_length_ptr = ffi.new("unsigned long*")
        string_name = encode("string")
        string = encode(_benchmark_db_get_identifier(i))
        selector = lib.j_db_selector_new(b_scheme, lib.J_DB_SELECTOR_MODE_AND,
                                         b_s_error_ptr)
        assert lib.j_db_selector_add_field(selector, string_name,
                                           lib.J_DB_SELECTOR_OPERATOR_EQ,
                                           string, 0, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        iterator = lib.j_db_iterator_new(b_scheme, selector, b_s_error_ptr)
        assert iterator != ffi.NULL
        assert b_s_error_ptr == ffi.NULL
        assert lib.j_db_iterator_next(iterator, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        assert lib.j_db_iterator_get_field(iterator, string_name, field_type_ptr,
                                       field_value_ptr, field_length_ptr,
                                       b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        lib.j_db_selector_unref(selector)
        lib.j_db_iterator_unref(iterator)
    run.operations = iterations
    lib.j_batch_unref(batch)
    lib.j_batch_unref(delete_batch)
    lib.j_db_schema_unref(b_scheme)

def benchmark_db_get_range(run):
    _benchmark_db_get_range(run, "benchmark_get_range", False, False)

def benchmark_db_get_range_index_single(run):
    _benchmark_db_get_range(run, "benchmark_get_range_index_single", False,
                             True)

def benchmark_db_get_range_index_all(run):
    _benchmark_db_get_range(run, "benchmark_get_range_index_all", True, False)

def benchmark_db_get_range_index_mixed(run):
    _benchmark_db_get_range(run, "benchmark_get_range_index_mixed", True, True)

def _benchmark_db_get_range(run, namespace, use_index_all, use_index_single):
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
    for i in range(N_GET_DIVIDER):
        field_type_ptr = ffi.new("JDBType*")
        field_value_ptr = ffi.new("void**")
        field_length_ptr = ffi.new("unsigned long*")
        sint_name = encode("sint")
        string_name = encode("string")
        range_begin_ptr = ffi.new("long*")
        range_begin_ptr[0] = int((i * (CLASS_MODULUS / N_GET_DIVIDER)) - CLASS_LIMIT)
        range_end_ptr = ffi.new("long*")
        range_end_ptr[0] = int(((i + 1) * (CLASS_MODULUS / N_GET_DIVIDER)) - (CLASS_LIMIT + 1))
        selector = lib.j_db_selector_new(b_scheme, lib.J_DB_SELECTOR_MODE_AND,
                                         b_s_error_ptr)
        assert lib.j_db_selector_add_field(selector, sint_name,
                                           lib.J_DB_SELECTOR_OPERATOR_GE,
                                           range_begin_ptr, 0 , b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        assert lib.j_db_selector_add_field(selector, sint_name,
                                           lib.J_DB_SELECTOR_OPERATOR_LE,
                                           range_end_ptr, 0, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        iterator = lib.j_db_iterator_new(b_scheme, selector, b_s_error_ptr)
        assert iterator != ffi.NULL
        assert b_s_error_ptr == ffi.NULL
        assert lib.j_db_iterator_next(iterator, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        assert lib.j_db_iterator_get_field(iterator, string_name, field_type_ptr,
                                       field_value_ptr, field_length_ptr,
                                       b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
    run.stop_timer()
    assert lib.j_batch_execute(delete_batch)
    run.operations = N_GET_DIVIDER
    lib.j_db_schema_unref(b_scheme)
    lib.j_batch_unref(batch)
    lib.j_batch_unref(delete_batch)
