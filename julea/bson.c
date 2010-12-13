#include <glib.h>

#include <bson.h>

#include "bson.h"

struct JBSON
{
	bson b;
	bson_buffer buf;
};

JBSON*
j_bson_new (void)
{
	JBSON* jbson;

	jbson = g_new(JBSON, 1);

	bson_buffer_init(&(jbson->buf));

	return jbson;
}

void
j_bson_free (JBSON* jbson)
{
	bson_destroy(&(jbson->b));
	bson_buffer_destroy(&(jbson->buf));
}

void
j_bson_append_int (JBSON* jbson, const gchar* key, gint value)
{
	bson_append_int(&(jbson->buf), key, value);
}

void
j_bson_append_str (JBSON* jbson, const gchar* key, const gchar* value)
{
	bson_append_string(&(jbson->buf), key, value);
}

bson*
j_bson_get (JBSON* jbson)
{
	bson_from_buffer(&(jbson->b), &(jbson->buf));

	return &(jbson->b);
}
