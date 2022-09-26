# Database Backend Code Details

This document describes some details on the JULEA database client, backend, their communication and the db-util library.

## Comments on the JULEA-DB API

- Joins and Adds are only allowed in the same namespace. (TODO add this to doc string)
- Modifying a selector A after joining/adding it to B does not update B.
- Joining/Adding selector A to B moves join and sub-selector information from A to B.
- While joining two selectors, fields of the respective primary schemas must be used.
- Extracting a field from a JDBIterator requires knowledge of the field's schema if joins are involved. Otherwise `NULL` can be passed.
- Adding selector A to B requires them to have the same primary schema.

## JULEA-DB Client-Server Communication

BSON documents are used to encode selectors or query results for network transfer.
The BSON documents for SELECT queries are of the following structure (for simplicity shown in JSON):

```text
<query> := { "t" : <tables>, "j" : <joins>, "s" : <selector> } | { "s" : <selector> }

<tables> := [ <table_name> <table_list> ]
<table_list> := , <table_name> <table_list> | ""

<joins> := [ <join> <join_list> ]
<join_list> := , <join> <join_list> | ""
<join> := { "<table_name>" : "<field_name>", "<table_name>" : "<field_name>" }

<selector> := <selector_non_empty> | { "m" : <mode> }
<selector_non_empty> := { "m" : <mode>, <condition> <condition_list> }
<condition_list> := , <condition> <condition_list> | ""
<condition> := "<field_name>" : { "t" : <table_name>, "o" : <operator>, "v" : <value> } | "_s" : <selector_non_empty>
```

For UPDATE and DELETE queries or simple queries without joins only the "s" KV pair is present in `<query>`.

Some points to note:

- Arrays in BSON are documents that use increasing numbers as keys.
  The DB-Client implementation relaxes this requirement for the `<join_list>` where the keys are set to "0" to ease the implementation.
- The namespaces for the different operations are passsed as an independent parameter.

The results of a query are passed as BSON documents of the following form:

```text
<results> := { <result> <result_list> } | {}
<result_list> := , <result> <result_list> | ""

<result> := <result_number> : <row>

<row> := { <variable> <variable_list> }
<variable_list> := , <variable> <variable_list>
<variable> := <full_field_name> : <value>
```

Results of a schema query are passed as BSON documents of the following form:

```text
<schema> := { <field> <field_list> }
<field_list> := , <field> <field_list> | ""

<field> := <field_name> : <field_type>
```

## The db-util Library

The db-util library contains wrappers for BSON functions and generic functions for interaction with SQL databses.
It is intended to ease the implementation and maintanance of SQL DB backends.
Implementing a DB backend using this library requires you to implement the needed callbacks for sql-generic and initialize the library in the backend initialize function using JSQLSpecifics.
Then, the respective `j_sql_*` functions can be used in a `JBackend` struct.
Examples are given by `backend/db/sqlite.c` and `backend/db/mysql.c`.
The library makes use of thread-local caches for prepared statements and schema information.

Requirements on the actual DB backend are:
- The DBMS should support prepared statements, otherwise this function needs to be faked by the provided backend functions.
- Values that are bound to prepared statements must still be bound after a reset.
