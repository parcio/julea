#ifndef H_BSON
#define H_BSON

struct JBSON;

typedef struct JBSON JBSON;

#include <glib.h>

#include <bson.h>

JBSON* j_bson_new (void);
void j_bson_free (JBSON*);

void j_bson_append_int (JBSON*, const gchar*, gint);
void j_bson_append_str (JBSON*, const gchar*, const gchar*);
bson* j_bson_get (JBSON*);

#endif
