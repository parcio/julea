from julea import JBatchResult, JBatch, ffi, lib, encode, read_from_buffer

if __name__ == "__main__":
    try:
        value_obj = encode("Hello Object!")
        value_kv = encode("Hello Key-Value!")
        value_db = encode("Hello Database!")

        result = JBatchResult()
        hello = encode("hello")
        world = encode("world")
        nbytes_ptr = ffi.new("unsigned long*")
        _object = lib.j_object_new(hello, world)
        kv = lib.j_kv_new(hello, world)
        schema = lib.j_db_schema_new(hello, world, ffi.NULL)
        lib.j_db_schema_add_field(schema, hello, lib.J_DB_TYPE_STRING, ffi.NULL)
        entry = lib.j_db_entry_new(schema, ffi.NULL)

        lib.j_db_entry_set_field(entry, hello, value_db, len(value_db), ffi.NULL)

        with JBatch(result) as batch:
            lib.j_object_create(_object, batch)
            lib.j_object_write(_object, value_obj, len(value_obj), 0, nbytes_ptr, batch)
            lib.j_kv_put(kv, value_kv, len(value_kv), ffi.NULL, batch)
            lib.j_db_schema_create(schema, batch, ffi.NULL)
            lib.j_db_entry_insert(entry, batch, ffi.NULL)

        if result.IsSuccess:
            result = JBatchResult()
            buffer = ffi.new("gchar[]", 128)
            with JBatch(result) as batch:
                lib.j_object_read(_object, buffer, 128, 0, nbytes_ptr, batch)
            if result.IsSuccess:
                print(f"Object contains: '{read_from_buffer(buffer)}' ({nbytes_ptr[0]} bytes)")
            with JBatch(result) as batch:
                lib.j_object_delete(_object, batch)

            result = JBatchResult()
            with JBatch(result) as batch:
                buffer_ptr = ffi.new("void**")
                length = ffi.new("unsigned int *")
                lib.j_kv_get(kv, buffer_ptr, length, batch)
            if result.IsSuccess:
                char_buff_ptr = ffi.cast("char**", buffer_ptr)
                print(f"KV contains: '{read_from_buffer(char_buff_ptr[0])}' ({length[0]} bytes)")
            with JBatch(result) as batch:
                lib.j_kv_delete(kv, batch)

            try:
                selector = lib.j_db_selector_new(schema, lib.J_DB_SELECTOR_MODE_AND, ffi.NULL)
                lib.j_db_selector_add_field(selector, hello, lib.J_DB_SELECTOR_OPERATOR_EQ, value_db, len(value_db), ffi.NULL)
                iterator = lib.j_db_iterator_new(schema, selector, ffi.NULL)
                
                while lib.j_db_iterator_next(iterator, ffi.NULL):
                    _type = ffi.new("JDBType *")
                    db_field_ptr = ffi.new("void**")
                    db_length_ptr = ffi.new("unsigned long*")
                    lib.j_db_iterator_get_field(iterator, hello, _type, db_field_ptr, db_length_ptr, ffi.NULL)
                    print(f"DB contains: '{read_from_buffer(db_field_ptr[0])}' ({db_length_ptr[0]} bytes)")

            finally:
                with JBatch(result) as batch:
                    lib.j_db_entry_delete(entry, selector, batch, ffi.NULL)
                    lib.j_db_schema_delete(schema, batch, ffi.NULL)
                lib.j_db_selector_unref(selector)
                lib.j_db_iterator_unref(iterator)

    finally:
        lib.j_kv_unref(kv)
        lib.j_object_unref(_object)
        lib.j_db_schema_unref(schema)
        lib.j_db_entry_unref(entry)
