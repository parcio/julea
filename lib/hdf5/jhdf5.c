/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017 Olga Perevalova
 * Copyright (C) 2017 Eugen Betke
 * Copyright (C) 2018-2019 Johannes Coym
 * Copyright (C) 2019 Michael Kuhn
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

// FIXME check whether version is up to date: https://github.com/Olgasnezh/hdf5-vol-sqlite-plugin
// FIXME clean up
// FIXME fix memory leaks

// FIXME
#define H5Sencode_vers 1

#include <julea-config.h>
#include <julea.h>
#include <julea-kv.h>
#include <julea-object.h>
#include <julea-internal.h>
#include <glib.h>
#include <bson.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

#include <hdf5.h>
#include <H5PLextern.h>

#define _GNU_SOURCE

#define JULEA 520

#define ERRORMSG(...)    \
	printf(__VA_ARGS__); \
	printf("\n");        \
	g_assert(false);       \
	exit(1);

#define COUNT_MAX 2147479552

struct JHF_t;

typedef struct JHF_t
{
	char *name;
} JHF_t; /* structure for file*/

typedef struct JHG_t
{
	char *location;
	char *name;
} JHG_t; /* structure for group*/

typedef struct JHD_t
{
	char *location;
	char *name;
	size_t data_size;
	JDistribution *distribution;
	JDistributedObject *object;
	JKV *kv;
} JHD_t; /* structure for dataset*/

typedef struct JHA_t
{
	char *location;
	char *name;
	size_t data_size;
	JKV *kv;
	JKV *ts;
} JHA_t; /*structure for attribute*/

typedef struct h5julea_fapl_t
{
	char *name;
} h5julea_fapl_t;

static herr_t H5VL_julea_fapl_free(void *info);
static void *H5VL_julea_fapl_copy(const void *info);

static herr_t H5VL_julea_init(hid_t vipl_id);
static herr_t H5VL_julea_term(void);

/* Atrribute callbacks */
static void *H5VL_julea_attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *attr_name, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req);
static void *H5VL_julea_attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *attr_name, hid_t aapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_julea_attr_read(void *attr, hid_t dtype_id, void *buf, hid_t dxpl_id, void **req);
static herr_t H5VL_julea_attr_write(void *attr, hid_t dtype_id, const void *buf, hid_t dxpl_id, void **req);
static herr_t H5VL_julea_attr_get(void *obj, H5VL_attr_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_julea_attr_close(void *attr, hid_t dxpl_id, void **req);

/* Dataset callbacks */
static void *H5VL_julea_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
static void *H5VL_julea_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_julea_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, void *buf, void **req);
static herr_t H5VL_julea_dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id, void **req, va_list arguments);
static herr_t H5VL_julea_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
static herr_t H5VL_julea_dataset_close(void *dset, hid_t dxpl_id, void **req);

/* File callbacks */
static void *H5VL_julea_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
static void *H5VL_julea_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_julea_file_close(void *file, hid_t dxpl_id, void **req);

/* Group callbacks */
static void *H5VL_julea_group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
static void *H5VL_julea_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_julea_group_close(void *grp, hid_t dxpl_id, void **req);

char *j_hdf5_encode_type(const char *, hid_t *, hid_t, size_t *);
char *j_hdf5_encode_space(const char *, hid_t *, hid_t, size_t *);

bson_t *j_hdf5_serialize(const void *, size_t);
bson_t *j_hdf5_serialize_ts (const void *, size_t, const void *, size_t);
bson_t *j_hdf5_serialize_dataset(const void *, size_t, const void *, size_t, size_t, JDistribution *);

void j_hdf5_deserialize(const bson_t *, void *, size_t);
void *j_hdf5_deserialize_type (const bson_t*);
void *j_hdf5_deserialize_space (const bson_t*);
void j_hdf5_deserialize_dataset(const bson_t *, JHD_t *, size_t *);
void j_hdf5_deserialize_size(const bson_t *, size_t *);

char *create_path(const char *, char *);

hid_t native_plugin_id = -1;

/**
 * The class providing the functions to HDF5
 **/
static const H5VL_class_t H5VL_julea_g = {
	0,
	JULEA,
	"julea",	  /* name */
	0,
	H5VL_julea_init, /* initialize */
	H5VL_julea_term, /* terminate */
	{
		sizeof(h5julea_fapl_t),
		H5VL_julea_fapl_copy,
		NULL,
		H5VL_julea_fapl_free,
		NULL,
		NULL
	},
	{
		NULL,
		NULL,
		NULL,
		NULL,
		NULL
	},
	{
		/* attribute_cls */
		H5VL_julea_attr_create, /* create */
		H5VL_julea_attr_open,   /* open */
		H5VL_julea_attr_read,   /* read */
		H5VL_julea_attr_write,  /* write */
		H5VL_julea_attr_get,	/* get */
		NULL,					 //H5VL_julea_attr_specific,              /* specific */
		NULL,					 //H5VL_julea_attr_optional,              /* optional */
		H5VL_julea_attr_close   /* close */
	},
	{
		/* dataset_cls */
		H5VL_julea_dataset_create, /* create */
		H5VL_julea_dataset_open,   /* open */
		H5VL_julea_dataset_read,   /* read */
		H5VL_julea_dataset_write,  /* write */
		H5VL_julea_dataset_get,	/* get */
		NULL,						//H5VL_julea_dataset_specific,          /* specific */
		NULL,						//H5VL_julea_dataset_optional,          /* optional */
		H5VL_julea_dataset_close   /* close */
	},
	{
		/* datatype_cls */
		NULL, //H5VL_julea_datatype_commit, /* commit */
		NULL, //H5VL_julea_datatype_open,   /* open */
		NULL, //H5VL_julea_datatype_get,	/* get_size */
		NULL,						 //H5VL_julea_datatype_specific,         /* specific */
		NULL,						 //H5VL_julea_datatype_optional,         /* optional */
		NULL, //H5VL_julea_datatype_close   /* close */
	},
	{
		/* file_cls */
		H5VL_julea_file_create, /* create */
		H5VL_julea_file_open,   /* open */
		NULL, //H5VL_julea_file_get,	/* get */
		NULL,					 //H5VL_julea_file_specific,            /* specific */
		NULL,					 //H5VL_julea_file_optional,            /* optional */
		H5VL_julea_file_close   /* close */
	},
	{
		/* group_cls */
		H5VL_julea_group_create, /* create */
		H5VL_julea_group_open,   /* open */
		NULL, //H5VL_julea_group_get,	/* get */
		NULL,					  //H5VL_julea_group_specific,           /* specific */
		NULL,					  //H5VL_julea_group_optional,           /* optional */
		H5VL_julea_group_close   /* close */
	},
	{
		/* link_cls */
		NULL, //H5VL_julea_link_create,                /* create */
		NULL, //H5VL_julea_link_copy,                  /* copy */
		NULL, //H5VL_julea_link_move,                  /* move */
		NULL, //H5VL_julea_link_get,                   /* get */
		NULL, //H5VL_julea_link_specific,              /* specific */
		NULL, //H5VL_julea_link_optional,              /* optional */
	},
	{
		/* object_cls */
		NULL, //H5VL_julea_object_open,                        /* open */
		NULL, //H5VL_julea_object_copy,                /* copy */
		NULL, //H5VL_julea_object_get,                 /* get */
		NULL, //H5VL_julea_object_specific,                    /* specific */
		NULL, //H5VL_julea_object_optional,            /* optional */
	},
	{
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL
	},
	NULL
};

char *err_msg = 0; /* pointer to an error string */

h5julea_fapl_t *ginfo = NULL;

/**
 * Copies the file operations
 *
 * \author Johannes Coym
 *
 * \param info The original file operations
 *
 * \return fapl_target The new file operations.
 **/
static void *H5VL_julea_fapl_copy(const void *info)
{
	const h5julea_fapl_t* fapl_source = (const h5julea_fapl_t*) info;
	h5julea_fapl_t* fapl_target = (h5julea_fapl_t*) malloc(sizeof(*fapl_target));
	memcpy(fapl_target, fapl_source, sizeof(*fapl_source));
	fapl_target->name  = fapl_source->name;
	return (void *)fapl_target;
}

/**
 * Frees the file operations(unused)
 *
 * \author Johannes Coym
 *
 * \return err Error
 **/
static herr_t H5VL_julea_fapl_free(void *info __attribute__((unused)))
{
	herr_t err = 0;
	return err;
}

/**
 * Initializes the plugin
 *
 * \author Johannes Coym
 *
 * \return err Error
 **/
static herr_t H5VL_julea_init(hid_t vipl_id __attribute__((unused)))
{
	native_plugin_id = H5VLget_connector_id("native");
	g_assert(native_plugin_id > 0);
	return 0;
}

/**
 * Terminates the plugin
 *
 * \author Johannes Coym
 *
 * \return err Error
 **/
static herr_t H5VL_julea_term(void)
{
	return 0;
}

/**
 * Encodes the type
 *
 * \author Johannes Coym
 *
 * \return type_buf The encoded type
 **/
char*
j_hdf5_encode_type (const char *property, hid_t *type_id, hid_t cpl_id, size_t *type_size)
{
	char* type_buf;
	H5Pget(cpl_id, property, type_id);
	g_assert(-1 != *type_id);
	H5Tencode(*type_id, NULL, type_size);
	type_buf = (char*) malloc(*type_size);
	H5Tencode(*type_id, type_buf, type_size);
	return type_buf;
}

/**
 * Encodes the space
 *
 * \author Johannes Coym
 *
 * \return type_buf The encoded space
 **/
char*
j_hdf5_encode_space (const char *property, hid_t *space_id, hid_t cpl_id, size_t *space_size)
{
	char* space_buf;
	H5Pget(cpl_id, property, space_id);
	g_assert(-1 != *space_id);
	H5Sencode(*space_id, NULL, space_size);
	space_buf = (char*) malloc(*space_size);
	H5Sencode(*space_id, space_buf, space_size);
	return space_buf;
}

/**
 * Serializes attribute data
 *
 * \author Johannes Coym
 *
 * \param data The data
 * \param size The size of the data
 *
 * \return b The serialized BSON
 **/
bson_t*
j_hdf5_serialize (const void *data, size_t data_size)
{
	bson_t* b;

	j_trace_enter(G_STRFUNC, NULL);

	b = bson_new();
	bson_init(b);

	bson_append_binary(b, "data", -1, BSON_SUBTYPE_BINARY, data, data_size);
	bson_append_int32(b, "size", -1, (int32_t)data_size);

	j_trace_leave(G_STRFUNC);

	return b;
}

/**
 * Serializes type and space data
 *
 * \author Johannes Coym
 *
 * \param type_data The type data
 * \param type_size The size of the type data
 * \param space_data The space data
 * \param space_size The size of the space data
 *
 * \return b The serialized BSON
 **/
bson_t*
j_hdf5_serialize_ts (const void *type_data, size_t type_size, const void *space_data, size_t space_size)
{
	bson_t* b;

	j_trace_enter(G_STRFUNC, NULL);

	b = bson_new();
	bson_init(b);

	bson_append_int32(b, "tsize", -1, (int32_t)type_size);
	bson_append_int32(b, "ssize", -1, (int32_t)space_size);
	bson_append_binary(b, "tdata", -1, BSON_SUBTYPE_BINARY, type_data, type_size);
	bson_append_binary(b, "sdata", -1, BSON_SUBTYPE_BINARY, space_data, space_size);

	j_trace_leave(G_STRFUNC);

	return b;
}

/**
 * Serializes all metadata from datasets
 *
 * \author Johannes Coym
 *
 * \param type_data The type data
 * \param type_size The size of the type data
 * \param space_data The space data
 * \param space_size The size of the space data
 * \param data_size The data size of the dataset
 * \param distribution The distribution of the dataset
 *
 * \return b The serialized BSON
 **/
bson_t*
j_hdf5_serialize_dataset (const void *type_data, size_t type_size, const void *space_data, size_t space_size, size_t data_size, JDistribution* distribution)
{
	bson_t* b;
	bson_t* b_distribution;

	j_trace_enter(G_STRFUNC, NULL);

	b_distribution = j_distribution_serialize(distribution);

	b = bson_new();
	bson_init(b);

	bson_append_int32(b, "tsize", -1, (int32_t)type_size);
	bson_append_int32(b, "ssize", -1, (int32_t)space_size);
	bson_append_binary(b, "tdata", -1, BSON_SUBTYPE_BINARY, type_data, type_size);
	bson_append_binary(b, "sdata", -1, BSON_SUBTYPE_BINARY, space_data, space_size);
	bson_append_int32(b, "size", -1, (int32_t)data_size);
	bson_append_document(b, "distribution", -1, b_distribution);

	bson_destroy(b_distribution);

	j_trace_leave(G_STRFUNC);

	return b;
}

/**
 * Deserializes only the data from the bson
 *
 * \author Johannes Coym
 *
 * \param b The bson containing the data
 * \param data The pointer where the data is returned
 * \param data_size The size of the data
 **/
void
j_hdf5_deserialize (const bson_t* b, void *data, size_t data_size)
{
	bson_iter_t iterator;
	const void *buf;
	bson_subtype_t bs;

	g_return_if_fail(b != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	bson_iter_init(&iterator, b);

	bs = BSON_SUBTYPE_BINARY;
	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "data") == 0)
		{
			bson_iter_binary(&iterator, &bs, (uint32_t *)&data_size, (const uint8_t **) &buf);
		}
	}
	memcpy(data, buf, data_size);
	j_trace_leave(G_STRFUNC);
}

/**
 * Deserializes the type data from the bson
 *
 * \author Johannes Coym
 *
 * \param b The bson containing the data
 *
 * \return type_data the encoded type data
 **/
void*
j_hdf5_deserialize_type (const bson_t* b)
{
	bson_iter_t iterator;
	const void *buf;
	bson_subtype_t bs;
	void* type_data;
	size_t type_size;

	j_trace_enter(G_STRFUNC, NULL);

	bson_iter_init(&iterator, b);
	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "tsize") == 0)
		{
			type_size = bson_iter_int32(&iterator);
			type_data = malloc(type_size);
		}
	}
	bson_iter_init(&iterator, b);
	bs = BSON_SUBTYPE_BINARY;
	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "tdata") == 0)
		{
			bson_iter_binary(&iterator, &bs, (uint32_t *)&type_size, (const uint8_t **) &buf);
		}
	}
	j_trace_leave(G_STRFUNC);
	memcpy(type_data, buf, type_size);
	return type_data;
}

/**
 * Deserializes the space data from the bson
 *
 * \author Johannes Coym
 *
 * \param b The bson containing the data
 *
 * \return space_data the encoded space data
 **/
void*
j_hdf5_deserialize_space (const bson_t* b)
{
	bson_iter_t iterator;
	const void *buf;
	bson_subtype_t bs;
	void* space_data;
	size_t space_size;

	j_trace_enter(G_STRFUNC, NULL);

	bson_iter_init(&iterator, b);
	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "ssize") == 0)
		{
			space_size = bson_iter_int32(&iterator);
			space_data = malloc(space_size);
		}
	}
	bson_iter_init(&iterator, b);
	bs = BSON_SUBTYPE_BINARY;
	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "sdata") == 0)
		{
			bson_iter_binary(&iterator, &bs, (uint32_t *)&space_size, (const uint8_t **) &buf);
		}
	}
	j_trace_leave(G_STRFUNC);
	memcpy(space_data, buf, space_size);
	return space_data;
}

/**
 * Deserializes only the data size from the bson
 *
 * \author Johannes Coym
 *
 * \param b The bson containing the data
 * \param data_size Pointer to the data size to return
 **/
void
j_hdf5_deserialize_size (const bson_t* b, size_t* data_size)
{
	bson_iter_t iterator;

	g_return_if_fail(data_size != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	bson_iter_init(&iterator, b);

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "size") == 0)
		{
			*data_size = bson_iter_int32(&iterator);
		}
	}
	j_trace_leave(G_STRFUNC);
}

/**
 * Deserializes data size and disttribution from the bson
 *
 * \author Johannes Coym
 *
 * \param b The bson containing the data
 * \param d The dataset whoch should contain the distribution
 * \param data_size Pointer to the data size to return
 **/
void
j_hdf5_deserialize_dataset (const bson_t* b, JHD_t* d, size_t* data_size)
{
	bson_iter_t iterator;

	j_trace_enter(G_STRFUNC, NULL);

	bson_iter_init(&iterator, b);

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "distribution") == 0)
		{
			guint8 const* data;
			guint32 len;
			bson_t b_distribution[1];

			bson_iter_document(&iterator, &len, &data);
			bson_init_static(b_distribution, data, len);
			d->distribution = j_distribution_new_from_bson(b_distribution);
			bson_destroy(b_distribution);
		}

		if (g_strcmp0(key, "size") == 0)
		{
			*data_size = bson_iter_int32(&iterator);
		}
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * Generates the path of the current element
 *
 * \author Johannes Coym
 *
 * \param name The name of the current element
 * \param prev_path The previous path
 *
 * \return path
 **/
char* create_path(const char* name, char* prev_path) {
	static const char* seperator = "/";
	char* path = NULL;

	path = (char*) malloc(strlen(prev_path) + strlen(seperator) + strlen(name) + 1);
	strcpy(path, prev_path);
	strcat(path, seperator);
	strcat(path, name);
	return path;
}

/**
 * Creates a new attribute
 *
 * \author Johannes Coym
 *
 * \return attribute The new attribute
 **/
static void *
H5VL_julea_attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *attr_name, hid_t acpl_id, hid_t aapl_id __attribute__((unused)), hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHA_t *attribute;
	hid_t space_id;
	size_t space_size;
	char* space_buf;
	int ndims;
	hsize_t *maxdims;
	hsize_t *dims;
	hid_t type_id;
	size_t type_size;
	char* type_buf;
	size_t data_size;
	bson_t* value;
	JBatch* batch;
	char* tsloc;

	attribute = (JHA_t *)malloc(sizeof(*attribute));

	type_buf = j_hdf5_encode_type("attr_type_id", &type_id, acpl_id, &type_size);
	space_buf = j_hdf5_encode_space("attr_space_id", &space_id, acpl_id, &space_size);

	ndims = H5Sget_simple_extent_ndims(space_id);
	maxdims = malloc(sizeof(hsize_t) * ndims);
	dims = malloc(sizeof(hsize_t) * ndims);
	H5Sget_simple_extent_dims(/* in */ space_id, /* out */ dims, /* out */ maxdims);
	data_size = H5Tget_size(type_id);

	for (int i = 0; i < ndims; ++i)
	{
		data_size *= dims[i];
	}

	switch (loc_params->obj_type)
	{
	case H5I_FILE:
		break;
	case H5I_GROUP:
	{
		JHG_t *o = (JHG_t *)obj;
		attribute->location = create_path(attr_name, o->location);

		j_trace_enter(G_STRFUNC, NULL);

		attribute->kv = j_kv_new("hdf5", attribute->location);

		j_trace_leave(G_STRFUNC);

		attribute->name = g_strdup(attr_name);
		attribute->data_size = data_size;
	}
	break;
	case H5I_DATATYPE:
		break;
	case H5I_DATASET:

	{
		JHD_t *o = (JHD_t *)obj;
		attribute->location = create_path(attr_name, o->location);

		j_trace_enter(G_STRFUNC, NULL);

		attribute->kv = j_kv_new("hdf5", attribute->location);

		j_trace_leave(G_STRFUNC);

		attribute->data_size = data_size;
		attribute->name = g_strdup(attr_name);
	}
	break;
	case H5I_ATTR:
		break;
	case H5I_UNINIT:
	case H5I_BADID:
	case H5I_DATASPACE:
	case H5I_VFL:
	case H5I_VOL:
	case H5I_GENPROP_CLS:
	case H5I_GENPROP_LST:
	case H5I_ERROR_CLASS:
	case H5I_ERROR_MSG:
	case H5I_ERROR_STACK:
	case H5I_NTYPES:
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	} /* end switch */

	// FIXME
	tsloc = (char*) malloc(strlen(attribute->location) + 4);

	j_trace_enter(G_STRFUNC, NULL);
	strcpy(tsloc, attribute->location);
	strcat(tsloc, "_ts");
	attribute->ts = j_kv_new("hdf5", tsloc);
	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = j_hdf5_serialize_ts(type_buf, type_size, space_buf, space_size);
	j_kv_put(attribute->ts, value, batch);
	j_batch_execute(batch);
	j_trace_leave(G_STRFUNC);

	return (void *)attribute;
}

/**
 * Opens an attribute
 *
 * \author Johannes Coym
 *
 * \return attribute The attribute
 **/
static void *
H5VL_julea_attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *attr_name,
					  hid_t aapl_id __attribute__((unused)), hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHA_t *attribute;
	JKV *kv;
	bson_t data[1];
	JBatch *batch;
	char* tsloc;

	attribute = (JHA_t *)malloc(sizeof(*attribute));
	attribute->name = g_strdup(attr_name);

	switch (loc_params->obj_type)
	{
	case H5I_FILE:
		break;
	case H5I_GROUP:
	{
		JHG_t *o = (JHG_t *)obj;
		attribute->location = create_path(attr_name, o->location);
	}
	break;
	case H5I_DATATYPE:
		break;
	case H5I_DATASET:
	{
		JHD_t *o = (JHD_t *)obj;
		attribute->location = create_path(attr_name, o->location);
	}
	break;
	case H5I_ATTR:
		break;
	case H5I_UNINIT:
	case H5I_BADID:
	case H5I_DATASPACE:
	case H5I_VFL:
	case H5I_VOL:
	case H5I_GENPROP_CLS:
	case H5I_GENPROP_LST:
	case H5I_ERROR_CLASS:
	case H5I_ERROR_MSG:
	case H5I_ERROR_STACK:
	case H5I_NTYPES:
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	} /* end switch */

	// FIXME
	tsloc = (char*) malloc(strlen(attribute->location) + 4);

	j_trace_enter(G_STRFUNC, NULL);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	kv = j_kv_new("hdf5", attribute->location);
	j_kv_get(kv, data, batch);

	strcpy(tsloc, attribute->location);
	strcat(tsloc, "_ts");
	attribute->ts = j_kv_new("hdf5", tsloc);
	if (j_batch_execute(batch))
	{
		j_hdf5_deserialize_size(data, &(attribute->data_size));
		attribute->kv = kv;
	}

	j_trace_leave(G_STRFUNC);

	return (void *)attribute;
}

/**
 * Reads the data from the attribute
 *
 * \author Johannes Coym
 **/
static herr_t
H5VL_julea_attr_read(void *attr, hid_t dtype_id __attribute__((unused)), void *buf, hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHA_t *d;
	bson_t b[1];
	JBatch* batch;
	d = (JHA_t *)attr;

	j_trace_enter(G_STRFUNC, NULL);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	j_kv_get(d->kv, b, batch);
	j_batch_execute(batch);
	j_hdf5_deserialize(b, buf, d->data_size);

	j_trace_leave(G_STRFUNC);

	return 1;
}

/**
 * Writes the data of the attribute
 *
 * \author Johannes Coym
 **/
static herr_t
H5VL_julea_attr_write(void *attr, hid_t dtype_id __attribute__((unused)), const void *buf, hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHA_t *d = (JHA_t *)attr;
	bson_t* value;
	JBatch* batch;

	j_trace_enter(G_STRFUNC, NULL);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
	value = j_hdf5_serialize(buf, d->data_size);
	j_kv_put(d->kv, value, batch);
	j_batch_execute(batch);

	j_trace_leave(G_STRFUNC);

	return 1;
}

/**
 * Provides get Functions of the attribute
 *
 * \author Johannes Coym
 *
 * \return ret_value The error code
 **/
static herr_t
H5VL_julea_attr_get(void *attr, H5VL_attr_get_t get_type, hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)), va_list arguments)
{
	herr_t ret_value = 0;
	JHA_t *d;
	bson_t b[1];
	JBatch* batch;

	d = (JHA_t *)attr;

	switch (get_type)
	{
	case H5VL_ATTR_GET_ACPL:
		break;
	case H5VL_ATTR_GET_INFO:
		break;
	case H5VL_ATTR_GET_NAME:
		break;
	case H5VL_ATTR_GET_SPACE:
	{
		hid_t* ret_id = va_arg (arguments, hid_t *);

		j_trace_enter(G_STRFUNC, NULL);
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_kv_get(d->ts, b, batch);
		j_batch_execute(batch);
		j_trace_leave(G_STRFUNC);
		*ret_id = H5Sdecode(j_hdf5_deserialize_space(b));
	}
	break;
	case H5VL_ATTR_GET_STORAGE_SIZE:
		break;
	case H5VL_ATTR_GET_TYPE:
	{
		hid_t* ret_id = va_arg (arguments, hid_t *);

		j_trace_enter(G_STRFUNC, NULL);
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_kv_get(d->ts, b, batch);
		j_batch_execute(batch);
		j_trace_leave(G_STRFUNC);
		*ret_id = H5Tdecode(j_hdf5_deserialize_type(b));
	}
	break;
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	}

	return ret_value;
}

/**
 * Closes the attribute
 *
 * \author Johannes Coym
 **/
static herr_t
H5VL_julea_attr_close(void *attr, hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHA_t *a = (JHA_t *)attr;

	if (a->kv != NULL)
	{
		j_kv_unref(a->kv);
	}

	if (a->ts != NULL)
	{
		j_kv_unref(a->ts);
	}

	g_free(a->name);
	a->name = NULL;
	free(a->location);
	a->location = NULL;
	free(a);
	return 1;
}

/**
 * Creates a new file
 *
 * \author Johannes Coym
 *
 * \return file The new file
 **/
static void *
H5VL_julea_file_create(const char *fname, unsigned flags __attribute__((unused)), hid_t fcpl_id __attribute__((unused)), hid_t fapl_id, hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHF_t *file;
	void *vol_info;

	H5Pget_vol_info(fapl_id, &vol_info);
	ginfo = (h5julea_fapl_t *)H5VL_julea_fapl_copy(vol_info);
	file = (JHF_t *)malloc(sizeof(*file));
	file->name = g_strdup(fname);
	return (void *)file;
}

/**
 * Opens a file
 *
 * \author Johannes Coym
 *
 * \return file The file
 **/
static void *
H5VL_julea_file_open(const char *fname, unsigned flags __attribute__((unused)), hid_t fapl_id, hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHF_t *file;
	void *vol_info;

	H5Pget_vol_info(fapl_id, &vol_info);
	ginfo = (h5julea_fapl_t *)H5VL_julea_fapl_copy(vol_info);
	file = (JHF_t *)malloc(sizeof(*file));
	file->name = g_strdup(fname);
	return (void *)file;
}

/**
 * Closes the file
 *
 * \author Johannes Coym
 **/
static herr_t
H5VL_julea_file_close(void *file, hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHF_t *f = (JHF_t *)file;
	g_free(f->name);
	f->name = NULL;
	free(f);
	H5VL_julea_fapl_free(ginfo);
	return 1;
}

/**
 * Creates a new group
 *
 * \author Johannes Coym
 *
 * \return group The new group
 **/
static void *
H5VL_julea_group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
						 hid_t gcpl_id __attribute__((unused)), hid_t gapl_id __attribute__((unused)), hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHG_t *group;
	group = (JHG_t *)malloc(sizeof(*group));

	switch (loc_params->obj_type)
	{
	case H5I_FILE:
	{
		JHF_t *o = (JHF_t *)obj;
		group->location = create_path(name, o->name);
		group->name = g_strdup(name);
	}
	break;
	case H5I_GROUP:
	{
		JHG_t *o = (JHG_t *)obj;
		group->location = create_path(name, o->location);
		group->name = g_strdup(name);
	}
	break;
	case H5I_DATATYPE:
		break;
	case H5I_DATASET:
		break;
	case H5I_ATTR:
		break;
	case H5I_UNINIT:
	case H5I_BADID:
	case H5I_DATASPACE:
	case H5I_VFL:
	case H5I_VOL:
	case H5I_GENPROP_CLS:
	case H5I_GENPROP_LST:
	case H5I_ERROR_CLASS:
	case H5I_ERROR_MSG:
	case H5I_ERROR_STACK:
	case H5I_NTYPES:
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	} /* end switch */
	return (void *)group;
}

/**
 * Opens a group
 *
 * \author Johannes Coym
 *
 * \return group The group
 **/
static void *H5VL_julea_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id __attribute__((unused)), hid_t dxpl_id __attribute__((unused)), void **req __attribute__((unused)))
{
	JHG_t *group;
	group = (JHG_t *)malloc(sizeof(*group));

	switch (loc_params->obj_type)
	{
	case H5I_FILE:
	{
		JHF_t *o = (JHF_t *)obj;
		group->location = create_path(name, o->name);
	}
	break;
	case H5I_GROUP:
	{
		JHG_t *o = (JHG_t *)obj;
		group->location = create_path(name, o->location);
	}
	break;
	case H5I_DATATYPE:
		break;
	case H5I_DATASET:
		break;
	case H5I_ATTR:
		break;
	case H5I_UNINIT:
	case H5I_BADID:
	case H5I_DATASPACE:
	case H5I_VFL:
	case H5I_VOL:
	case H5I_GENPROP_CLS:
	case H5I_GENPROP_LST:
	case H5I_ERROR_CLASS:
	case H5I_ERROR_MSG:
	case H5I_ERROR_STACK:
	case H5I_NTYPES:
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	} /* end switch */

	group->name = g_strdup(name);
	return (void *)group;
}

/**
 * Closes the group
 *
 * \author Johannes Coym
 **/
static herr_t
H5VL_julea_group_close(void *grp, hid_t dxpl_id  __attribute__((unused)), void **req  __attribute__((unused)))
{
	JHG_t *g = (JHG_t *)grp;
	g_free(g->name);
	free(g->location);
	free(g);
	return 1;
}

/**
 * Creates a new dataset
 *
 * \author Johannes Coym
 *
 * \return dset The new dataset
 **/
static void *
H5VL_julea_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dcpl_id, hid_t dapl_id  __attribute__((unused)), hid_t dxpl_id  __attribute__((unused)), void **req  __attribute__((unused)))
{
	JHD_t *dset;
	hid_t space_id;
	size_t space_size;
	char* space_buf;
	int ndims;
	hsize_t *maxdims;
	hsize_t *dims;
	hid_t type_id;
	size_t type_size;
	char* type_buf;
	size_t data_size;
	bson_t* value;
	JBatch* batch;
	char* tsloc;

	dset = (JHD_t *)malloc(sizeof(*dset));

	type_buf = j_hdf5_encode_type("dataset_type_id", &type_id, dcpl_id, &type_size);
	space_buf = j_hdf5_encode_space("dataset_space_id", &space_id, dcpl_id, &space_size);

	ndims = H5Sget_simple_extent_ndims(space_id);
	maxdims = malloc(sizeof(hsize_t) * ndims);
	dims = malloc(sizeof(hsize_t) * ndims);
	H5Sget_simple_extent_dims(/* in */ space_id, /* out */ dims, /* out */ maxdims);
	data_size = H5Tget_size(type_id);
	for (int i = 0; i < ndims; ++i)
	{
		data_size *= dims[i];
	}
	dset->data_size = data_size;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	switch (loc_params->obj_type)
	{
	case H5I_FILE:
	{
		JHF_t *o = (JHF_t *)obj;
		dset->location = create_path(name, o->name);

		j_trace_enter(G_STRFUNC, NULL);

		dset->distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN);
		dset->object = j_distributed_object_new("hdf5", dset->location, dset->distribution);
		j_distributed_object_create(dset->object, batch);

		j_trace_leave(G_STRFUNC);
	}

	break;
	case H5I_GROUP:
	{
		JHG_t *o = (JHG_t *)obj;
		dset->location = create_path(name, o->location);

		j_trace_enter(G_STRFUNC, NULL);

		dset->distribution = j_distribution_new(J_DISTRIBUTION_ROUND_ROBIN);
		dset->object = j_distributed_object_new("hdf5", dset->location, dset->distribution);
		j_distributed_object_create(dset->object, batch);

		j_trace_leave(G_STRFUNC);
	}
	break;
	case H5I_DATATYPE:
		break;
	case H5I_DATASET:
		break;
	case H5I_ATTR:
		break;
	case H5I_UNINIT:
	case H5I_BADID:
	case H5I_DATASPACE:
	case H5I_VFL:
	case H5I_VOL:
	case H5I_GENPROP_CLS:
	case H5I_GENPROP_LST:
	case H5I_ERROR_CLASS:
	case H5I_ERROR_MSG:
	case H5I_ERROR_STACK:
	case H5I_NTYPES:
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	} /* end switch */

	// FIXME
	tsloc = (char*) malloc(strlen(dset->location) + 6);

	j_trace_enter(G_STRFUNC, NULL);
	strcpy(tsloc, dset->location);
	strcat(tsloc, "_data");
	dset->kv = j_kv_new("hdf5", tsloc);
	value = j_hdf5_serialize_dataset(type_buf, type_size, space_buf, space_size, data_size, dset->distribution);
	j_kv_put(dset->kv, value, batch);
	j_batch_execute(batch);
	j_trace_leave(G_STRFUNC);

	dset->name = g_strdup(name);
	return (void *)dset;
}

/**
 * Opens a dataset
 *
 * \author Johannes Coym
 *
 * \return dset The dataset
 **/
static void *
H5VL_julea_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t dapl_id  __attribute__((unused)), hid_t dxpl_id  __attribute__((unused)), void **req  __attribute__((unused)))
{
	JHD_t *dset;
	JBatch *batch;
	bson_t kvdata[1];
	char* tsloc;

	dset = (JHD_t *)malloc(sizeof(*dset));
	dset->name = g_strdup(name);

	switch (loc_params->obj_type)
	{
	case H5I_FILE:
	{
		JHF_t *o = (JHF_t *)obj;
		dset->location = create_path(name, o->name);
	}

	break;
	case H5I_GROUP:
	{
		JHG_t *o = (JHG_t *)obj;
		dset->location = create_path(name, o->location);
	}
	break;
	case H5I_DATATYPE:
		break;
	case H5I_DATASET:
		break;
	case H5I_ATTR:
		break;
	case H5I_UNINIT:
	case H5I_BADID:
	case H5I_DATASPACE:
	case H5I_VFL:
	case H5I_VOL:
	case H5I_GENPROP_CLS:
	case H5I_GENPROP_LST:
	case H5I_ERROR_CLASS:
	case H5I_ERROR_MSG:
	case H5I_ERROR_STACK:
	case H5I_NTYPES:
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	} /* end switch */

	// FIXME
	tsloc = (char*) malloc(strlen(dset->location) + 6);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	strcpy(tsloc, dset->location);
	strcat(tsloc, "_data");
	dset->kv = j_kv_new("hdf5", tsloc);
	j_kv_get(dset->kv, kvdata, batch);
	if (j_batch_execute(batch))
	{
		j_hdf5_deserialize_dataset(kvdata, dset, &(dset->data_size));
	}

	dset->object = j_distributed_object_new("hdf5", dset->location, dset->distribution);

	return (void *)dset;
}

/**
 * Reads the data from the dataset
 *
 * \author Johannes Coym
 **/
static herr_t
H5VL_julea_dataset_read(void *dset, hid_t mem_type_id  __attribute__((unused)), hid_t mem_space_id  __attribute__((unused)),
						 hid_t file_space_id  __attribute__((unused)), hid_t plist_id  __attribute__((unused)), void *buf, void **req  __attribute__((unused)))
{
	JBatch* batch;
	JHD_t *d;
	guint64 bytes_read;

	d = (JHD_t *)dset;

	j_trace_enter(G_STRFUNC, NULL);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	bytes_read = 0;

	g_assert(buf != NULL);

	g_assert(d->object != NULL);

	j_distributed_object_read(d->object, buf, d->data_size, 0, &bytes_read, batch);

	j_batch_execute(batch);

	j_trace_leave(G_STRFUNC);

	return 1;
}

/**
 * Provides get Functions of the dataset
 *
 * \author Johannes Coym
 *
 * \return ret_value The error code
 **/
static herr_t
H5VL_julea_dataset_get(void *dset, H5VL_dataset_get_t get_type, hid_t dxpl_id  __attribute__((unused)), void **req  __attribute__((unused)), va_list arguments)
{
	herr_t ret_value = 0;
	JHD_t *d;
	bson_t b[1];
	JBatch* batch;

	d = (JHD_t *)dset;

	switch (get_type)
	{
	case H5VL_DATASET_GET_DAPL:
		break;
	case H5VL_DATASET_GET_DCPL:
		break;
	case H5VL_DATASET_GET_OFFSET:
		break;
	case H5VL_DATASET_GET_SPACE:
	{
		hid_t* ret_id = va_arg (arguments, hid_t *);

		j_trace_enter(G_STRFUNC, NULL);
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_kv_get(d->kv, b, batch);
		j_batch_execute(batch);
		j_trace_leave(G_STRFUNC);
		*ret_id = H5Sdecode(j_hdf5_deserialize_space(b));
	}
		break;
	case H5VL_DATASET_GET_SPACE_STATUS:
		break;
	case H5VL_DATASET_GET_STORAGE_SIZE:
		break;
	case H5VL_DATASET_GET_TYPE:
	{
		hid_t* ret_id = va_arg (arguments, hid_t *);

		j_trace_enter(G_STRFUNC, NULL);
		batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
		j_kv_get(d->kv, b, batch);
		j_batch_execute(batch);
		j_trace_leave(G_STRFUNC);
		*ret_id = H5Tdecode(j_hdf5_deserialize_type(b));
	}
		break;
	default:
		printf("ERROR: unsupported type %s:%d\n", __FILE__, __LINE__);
		exit(1);
	}

	return ret_value;
}

/**
 * Writes the data to the dataset
 *
 * \author Johannes Coym
 **/
static herr_t
H5VL_julea_dataset_write(void *dset, hid_t mem_type_id  __attribute__((unused)), hid_t mem_space_id  __attribute__((unused)),
						  hid_t file_space_id  __attribute__((unused)), hid_t plist_id  __attribute__((unused)), const void *buf, void **req  __attribute__((unused)))
{
	JBatch* batch;
	JHD_t *d;
	guint64 bytes_written;

	d = (JHD_t *)dset;

	j_trace_enter(G_STRFUNC, NULL);

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	bytes_written = 0;

	j_distributed_object_write(d->object, buf, d->data_size, 0, &bytes_written, batch);

	j_batch_execute(batch);

	j_trace_leave(G_STRFUNC);

	return 1;
}

/**
 * Closes the dataset
 *
 * \author Johannes Coym
 **/
static herr_t
H5VL_julea_dataset_close(void *dset, hid_t dxpl_id  __attribute__((unused)), void **req  __attribute__((unused)))
{
	JHD_t *d = (JHD_t *)dset;
	if (d->distribution != NULL)
	{
		j_distribution_unref(d->distribution);
	}

	if (d->kv != NULL)
	{
		j_kv_unref(d->kv);
	}

	if (d->object != NULL)
	{
		j_distributed_object_unref(d->object);
	}

	g_free(d->name);
	free(d->location);
	free(d);
	return 1;
}

/**
 * Provides the plugin type
 *
 * \author Johannes Coym
 **/
H5PL_type_t
H5PLget_plugin_type (void)
{
	return H5PL_TYPE_VOL;
}

/**
 * Provides a pointer to the plugin structure
 *
 * \author Johannes Coym
 **/
const void*
H5PLget_plugin_info (void)
{
	return &H5VL_julea_g;
}
