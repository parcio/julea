#ifndef SQL_GENERIC_BACKEND_INTERNAL_H
#define SQL_GENERIC_BACKEND_INTERNAL_H

struct JThreadVariables
{
	gboolean initialized;
	void* sql_backend;
	GHashTable* namespaces;
};

typedef struct JThreadVariables JThreadVariables;

struct JSqlCacheNames
{
	GHashTable* names;
};

typedef struct JSqlCacheNames JSqlCacheNames;

struct JSqlCacheSQLQueries
{
	GHashTable* types; // variablename(char*) -> variabletype(JDBType)
	GHashTable* queries; //sql(char*) -> (JSqlCacheSQLPrepared*)
};

typedef struct JSqlCacheSQLQueries JSqlCacheSQLQueries;

struct JSqlCacheSQLPrepared
{
	GString* sql;
	void* stmt;
	guint variables_count;
	GHashTable* variables_index;
	gboolean initialized;
	gchar* namespace;
	gchar* name;
	gpointer backend_data;
};

typedef struct JSqlCacheSQLPrepared JSqlCacheSQLPrepared;

struct JSqlBatch
{
	const gchar* namespace;
	JSemantics* semantics;
	gboolean open;
	gboolean aborted;
};

typedef struct JSqlBatch JSqlBatch;

struct JSqlIterator
{
	char* namespace;
	char* name;
	GArray* arr;
	guint index;
};

typedef struct JSqlIterator JSqlIterator;

#endif
