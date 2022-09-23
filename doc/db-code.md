# SQL Generic Library

// TODO

The db-util library contains wrapper for bson functions and generic functions to ease the implementation of new SQL based DB backends.
Implementing a DB backend using this library requires to implement the needed callbacks for sql-generic and initialize the library using JSQLSpecifics.
Examples are given in /backend/db/sqlite.c and /backend/db/mysql.c

## Comments on the JULEA-DB API

- A used selector must never be modified but can be reused.
- Modifying a selector A after joining/adding it to B does not update B.
- Joins and Adds are only allowed in the same namespace.
- Joining/Adding selector A to B moves join and sub-selector information from A to B.
- While joining two selectors, fields of the respective primary schemas must be used.
- Joing a selector A to B also adds A to B.
- Extracting a field from a query response requires knowledge of the schema the field belongs to if joins are involved. Otherwise `NULL` can be passed.
- Adding selector A to B requires them to have the same primary schema.
- Adding a selector A to a selector B moves join information from A to B.

## Internals of JULEA-DB Client-Server Communication

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

For UPDATE and DELETE queries or simple queries without joins only the "s" KV pair is present at top level.

Some points to note:

- Arrays in BSON are documents that use increasing numbers as keys.
  The DB-Client implementation relaxes this requirement for the `<join_list>` where the keys are set to "0" to ease the implementation.
- The namespaces for the different operations are passsed as an independent parameter.

## Assumptions of sql-generic on the SQL-DBMS

- The DBMS should support prepared statements, otherwise this function needs to be faked by the provided backend functions.
- Values that are bound to prepared statements must still be bound after a reset.

## TODOS (remove this section before PR)

- check that no debug prints are left in the code (including out-commented ones)
- check all doc strings
- finish this document
- client-side error for selector modify after use or regenerate final
