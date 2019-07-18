/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <bson.h>

#include <julea.h>
#include <julea-internal.h>
#include <db/jdb-internal.h>
#include <julea-db.h>

JDBSelector*
j_db_selector_new(JDBSchema* schema, GError** error)
{
	JDBSelector* selector;
	selector = g_slice_new(JDBSelector);
	selector->ref_count = 1;
	bson_init(&selector->bson);
	selector->schema = j_db_schema_ref(schema, error);
	j_goto_error_subcommand(!selector->schema);
	return selector;
_error:
	j_db_selector_unref(selector);
	return NULL;
}
JDBSelector*
j_db_selector_ref(JDBSelector* selector, GError** error)
{
	j_goto_error_frontend(!selector, JULEA_FRONTEND_ERROR_SELECTOR_NULL, "");
	g_atomic_int_inc(&selector->ref_count);
	return selector;
_error:
	return FALSE;
}
void
j_db_selector_unref(JDBSelector* selector)
{
	if (selector && g_atomic_int_dec_and_test(&selector->ref_count))
	{
		bson_destroy(&selector->bson);
		g_slice_free(JDBSelector, selector);
	}
}
