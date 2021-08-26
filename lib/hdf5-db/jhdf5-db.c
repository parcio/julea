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

/// \todo
#define H5Sencode_vers 1

#include <julea-config.h>

#include <glib.h>

#include <hdf5.h>
#include <H5PLextern.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <hdf5/jhdf5.h>

#include <julea.h>
#include <julea-db.h>
#include <julea-object.h>

#include "jhdf5-db.h"

static herr_t H5VL_julea_db_attr_init(hid_t vipl_id);
static herr_t H5VL_julea_db_attr_term(void);
static herr_t H5VL_julea_db_dataset_init(hid_t vipl_id);
static herr_t H5VL_julea_db_dataset_term(void);
static herr_t H5VL_julea_db_datatype_init(hid_t vipl_id);
static herr_t H5VL_julea_db_datatype_term(void);
static herr_t H5VL_julea_db_file_init(hid_t vipl_id);
static herr_t H5VL_julea_db_file_term(void);
static herr_t H5VL_julea_db_group_init(hid_t vipl_id);
static herr_t H5VL_julea_db_group_term(void);
static herr_t H5VL_julea_db_space_init(hid_t vipl_id);
static herr_t H5VL_julea_db_space_term(void);
static herr_t H5VL_julea_db_link_init(hid_t vipl_id);
static herr_t H5VL_julea_db_link_term(void);

#include "jhdf5-db.h"

/// \todo order is important
#include "jhdf5-db-shared.c"
#include "jhdf5-db-link.c"
#include "jhdf5-db-group.c"
#include "jhdf5-db-datatype.c"
#include "jhdf5-db-space.c"
#include "jhdf5-db-attr.c"
#include "jhdf5-db-dataset.c"
#include "jhdf5-db-file.c"

#define _GNU_SOURCE

#define JULEA_DB 530

static herr_t
H5VL_julea_db_init(hid_t vipl_id)
{
	J_TRACE_FUNCTION(NULL);

	if (H5VL_julea_db_file_init(vipl_id))
	{
		G_DEBUG_HERE();
		goto _error_file;
	}

	if (H5VL_julea_db_dataset_init(vipl_id))
	{
		G_DEBUG_HERE();
		goto _error_dataset;
	}

	if (H5VL_julea_db_attr_init(vipl_id))
	{
		G_DEBUG_HERE();
		goto _error_attr;
	}

	if (H5VL_julea_db_datatype_init(vipl_id))
	{
		G_DEBUG_HERE();
		goto _error_datatype;
	}

	if (H5VL_julea_db_group_init(vipl_id))
	{
		G_DEBUG_HERE();
		goto _error_group;
	}

	if (H5VL_julea_db_space_init(vipl_id))
	{
		G_DEBUG_HERE();
		goto _error_space;
	}

	if (H5VL_julea_db_link_init(vipl_id))
	{
		G_DEBUG_HERE();
		goto _error_link;
	}

	return 0;

_error_link:
	H5VL_julea_db_link_term();

_error_space:
	H5VL_julea_db_space_term();

_error_group:
	H5VL_julea_db_group_term();

_error_datatype:
	H5VL_julea_db_datatype_term();

_error_attr:
	H5VL_julea_db_attr_term();

_error_dataset:
	H5VL_julea_db_dataset_term();

_error_file:
	H5VL_julea_db_file_term();

	return 1;
}

static herr_t
H5VL_julea_db_term(void)
{
	J_TRACE_FUNCTION(NULL);

	if (H5VL_julea_db_link_term())
	{
		j_goto_error();
	}

	if (H5VL_julea_db_space_term())
	{
		j_goto_error();
	}

	if (H5VL_julea_db_group_term())
	{
		j_goto_error();
	}

	if (H5VL_julea_db_datatype_term())
	{
		j_goto_error();
	}

	if (H5VL_julea_db_attr_term())
	{
		j_goto_error();
	}

	if (H5VL_julea_db_dataset_term())
	{
		j_goto_error();
	}

	if (H5VL_julea_db_file_term())
	{
		j_goto_error();
	}

	return 0;

_error:
	return 1;
}

static const H5VL_class_t H5VL_julea_db_g;

static herr_t
H5VL_julea_db_introspect_get_conn_cls(void* obj, H5VL_get_conn_lvl_t lvl, const struct H5VL_class_t** conn_cls)
{
	(void)obj;
	(void)lvl;

	*conn_cls = &H5VL_julea_db_g;

	return 0;
}

static herr_t
H5VL_julea_db_introspect_opt_query(void* obj, H5VL_subclass_t cls, int opt_type, hbool_t* supported)
{
	(void)obj;
	(void)cls;
	(void)opt_type;

	*supported = FALSE;

	return 0;
}

/**
 * The class providing the functions to HDF5
 **/
static const H5VL_class_t H5VL_julea_db_g = {
	.version = 0,
	.value = JULEA_DB,
	.name = "julea-db",
	.cap_flags = 0,
	.initialize = H5VL_julea_db_init,
	.terminate = H5VL_julea_db_term,
	.info_cls = {
		.size = 0,
		.copy = NULL,
		.cmp = NULL,
		.free = NULL,
		.to_str = NULL,
		.from_str = NULL,
	},
	.wrap_cls = {
		.get_object = NULL,
		.get_wrap_ctx = NULL,
		.wrap_object = NULL,
		.unwrap_object = NULL,
		.free_wrap_ctx = NULL,
	},
	.attr_cls = {
		.create = H5VL_julea_db_attr_create,
		.open = H5VL_julea_db_attr_open,
		.read = H5VL_julea_db_attr_read,
		.write = H5VL_julea_db_attr_write,
		.get = H5VL_julea_db_attr_get,
		.specific = H5VL_julea_db_attr_specific,
		.optional = H5VL_julea_db_attr_optional,
		.close = H5VL_julea_db_attr_close,
	},
	.dataset_cls = {
		.create = H5VL_julea_db_dataset_create,
		.open = H5VL_julea_db_dataset_open,
		.read = H5VL_julea_db_dataset_read,
		.write = H5VL_julea_db_dataset_write,
		.get = H5VL_julea_db_dataset_get,
		.specific = H5VL_julea_db_dataset_specific,
		.optional = H5VL_julea_db_dataset_optional,
		.close = H5VL_julea_db_dataset_close,
	},
	.datatype_cls = {
		.commit = H5VL_julea_db_datatype_commit,
		.open = H5VL_julea_db_datatype_open,
		.get = H5VL_julea_db_datatype_get,
		.specific = H5VL_julea_db_datatype_specific,
		.optional = H5VL_julea_db_datatype_optional,
		.close = H5VL_julea_db_datatype_close,
	},
	.file_cls = {
		.create = H5VL_julea_db_file_create,
		.open = H5VL_julea_db_file_open,
		.get = H5VL_julea_db_file_get,
		.specific = H5VL_julea_db_file_specific,
		.optional = H5VL_julea_db_file_optional,
		.close = H5VL_julea_db_file_close,
	},
	.group_cls = {
		.create = H5VL_julea_db_group_create,
		.open = H5VL_julea_db_group_open,
		.get = H5VL_julea_db_group_get,
		.specific = H5VL_julea_db_group_specific,
		.optional = H5VL_julea_db_group_optional,
		.close = H5VL_julea_db_group_close,
	},
	.link_cls = {
		.create = H5VL_julea_db_link_create,
		.copy = H5VL_julea_db_link_copy,
		.move = H5VL_julea_db_link_move,
		.get = H5VL_julea_db_link_get,
		.specific = H5VL_julea_db_link_specific,
		.optional = H5VL_julea_db_link_optional,
	},
	.object_cls = {
		.open = NULL,
		.copy = NULL,
		.get = NULL,
		.specific = NULL,
		.optional = NULL,
	},
	.introspect_cls = {
		.get_conn_cls = H5VL_julea_db_introspect_get_conn_cls,
		.opt_query = H5VL_julea_db_introspect_opt_query,
	},
	.request_cls = {
		.wait = NULL,
		.notify = NULL,
		.cancel = NULL,
		.specific = NULL,
		.optional = NULL,
		.free = NULL,
	},
	.blob_cls = {
		.put = NULL,
		.get = NULL,
		.specific = NULL,
		.optional = NULL,
	},
	.token_cls = {
		.cmp = NULL,
		.to_str = NULL,
		.from_str = NULL,
	},
	.optional = NULL
};

/**
 * Provides the plugin type
 **/
H5PL_type_t
H5PLget_plugin_type(void)
{
	return H5PL_TYPE_VOL;
}

/**
 * Provides a pointer to the plugin structure
 **/
const void*
H5PLget_plugin_info(void)
{
	return &H5VL_julea_db_g;
}

/// \todo copy and use GLib's G_DEFINE_CONSTRUCTOR/DESTRUCTOR
static void __attribute__((constructor)) j_hdf5_init(void);
static void __attribute__((destructor)) j_hdf5_fini(void);

static void
j_hdf5_init(void)
{
}

static void
j_hdf5_fini(void)
{
}

void
j_hdf5_set_semantics(JSemantics* semantics)
{
	(void)semantics;

	/// \todo implement this
}
