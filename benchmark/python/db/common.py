from julea import lib, encode, ffi

N = 1 << 12
N_GET_DIVIDER = N >> 8
N_PRIME = 11971
N_MODULUS = 256
CLASS_MODULUS = N >> 3
CLASS_LIMIT = CLASS_MODULUS >> 1
SIGNED_FACTOR = N_PRIME
USIGNED_FACTOR = N_PRIME
FLOAT_FACTOR = 3.1415926

def _benchmark_db_prepare_scheme(namespace_encoded, use_batch, use_index_all,
                                 use_index_single, batch, delete_batch):
    b_s_error_ptr = ffi.new("GError*")
    b_s_error_ptr = ffi.NULL
    table_name = encode("table")
    string_name = encode("string")
    float_name = encode("float")
    uint_name = encode("uint")
    sint_name = encode("sint")
    blob_name = encode("blob")
    b_scheme = lib.j_db_schema_new(namespace_encoded, table_name, b_s_error_ptr)
    assert b_scheme != ffi.NULL
    assert b_s_error_ptr == ffi.NULL
    assert lib.j_db_schema_add_field(b_scheme, string_name,
                                     lib.J_DB_TYPE_STRING, b_s_error_ptr)
    assert b_s_error_ptr == ffi.NULL
    assert lib.j_db_schema_add_field(b_scheme, float_name,
                                     lib.J_DB_TYPE_FLOAT64, b_s_error_ptr)
    assert b_s_error_ptr == ffi.NULL
    assert lib.j_db_schema_add_field(b_scheme, uint_name, lib.J_DB_TYPE_UINT64,
                                     b_s_error_ptr)
    assert b_s_error_ptr == ffi.NULL
    assert lib.j_db_schema_add_field(b_scheme, sint_name, lib.J_DB_TYPE_UINT64,
                                     b_s_error_ptr)
    assert b_s_error_ptr == ffi.NULL
    assert lib.j_db_schema_add_field(b_scheme, blob_name, lib.J_DB_TYPE_BLOB,
                                     b_s_error_ptr)
    assert b_s_error_ptr == ffi.NULL
    if use_index_all:
        names = ffi.new("char*[5]")
        names[0] = encode("string")
        names[1] = encode("float")
        names[2] = encode("uint")
        names[3] = encode("sint")
        names[4] = ffi.NULL
        assert lib.j_db_schema_add_index(b_scheme, names, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
    if use_index_single:
        names = ffi.new("char*[2]")
        names[1] = ffi.NULL
        names[0] = encode("string")
        assert lib.j_db_schema_add_index(b_scheme, names, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        names[0] = encode("float")
        assert lib.j_db_schema_add_index(b_scheme, names, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        names[0] = encode("uint")
        assert lib.j_db_schema_add_index(b_scheme, names, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        names[0] = encode("sint")
        assert lib.j_db_schema_add_index(b_scheme, names, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
    assert lib.j_db_schema_create(b_scheme, batch, b_s_error_ptr)
    assert b_s_error_ptr == ffi.NULL
    assert lib.j_db_schema_delete(b_scheme, delete_batch, b_s_error_ptr)
    assert b_s_error_ptr == ffi.NULL
    if not use_batch:
        assert lib.j_batch_execute(batch)
    return lib.j_db_schema_ref(b_scheme)

def _benchmark_db_get_identifier(i):
    return f"{i * SIGNED_FACTOR % N_MODULUS:x}-benchmark-{i}"

def _benchmark_db_insert(run, scheme, namespace, use_batch, use_index_all,
                         use_index_single, use_timer):
    b_scheme = None
    b_s_error_ptr = ffi.new("GError*")
    b_s_error_ptr = ffi.NULL
    namespace_encoded = encode(namespace)
    batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    delete_batch = lib.j_batch_new_for_template(lib.J_SEMANTICS_TEMPLATE_DEFAULT)
    if use_timer:
        assert scheme == None
        assert run != None
        b_scheme = _benchmark_db_prepare_scheme(namespace_encoded, use_batch,
                                                use_index_all, use_index_single,
                                                batch, delete_batch)
        assert b_scheme != None
        run.start_timer()
    else:
        assert use_batch
        assert run == None
        lib.j_db_schema_ref(scheme)
        b_scheme = scheme
    for i in range(N):
        i_signed_ptr = ffi.new("long*")
        i_signed_ptr[0] = ((i * SIGNED_FACTOR) % CLASS_MODULUS) - CLASS_LIMIT
        i_usigned_ptr = ffi.new("unsigned long*")
        i_usigned_ptr[0] = ((i * USIGNED_FACTOR) % CLASS_MODULUS)
        i_float_ptr = ffi.new("double*")
        i_float_ptr[0] = i_signed_ptr[0] * FLOAT_FACTOR
        string = encode(_benchmark_db_get_identifier(i))
        string_name = encode("string")
        float_name = encode("float")
        sint_name = encode("sint")
        uint_name = encode("uint")
        entry = lib.j_db_entry_new(b_scheme, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        assert lib.j_db_entry_set_field(entry, string_name, string, 0,
                                        b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        assert lib.j_db_entry_set_field(entry, float_name, i_float_ptr, 0,
                                        b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        assert lib.j_db_entry_set_field(entry, sint_name, i_signed_ptr, 0,
                                        b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        assert lib.j_db_entry_set_field(entry, uint_name, i_usigned_ptr, 0,
                                        b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        assert lib.j_db_entry_insert(entry, batch, b_s_error_ptr)
        assert b_s_error_ptr == ffi.NULL
        if not use_batch:
            assert lib.j_batch_execute(batch)
    if use_batch or not use_timer:
        lib.j_batch_execute(batch)
    if use_timer:
        run.stop_timer()
        assert lib.j_batch_execute(delete_batch)
        run.operations = N
    lib.j_batch_unref(batch)
    lib.j_batch_unref(delete_batch)
    lib.j_db_entry_unref(entry)
    lib.j_db_schema_unref(b_scheme)
