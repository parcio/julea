/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2021 Timm Leon Erxleben
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


#include <julea-config.h>

#include <glib.h>
#include <hdf5.h>

#include <julea.h>


typedef struct JHDF5CopyParam_t JHDF5CopyParam_t;
struct JHDF5CopyParam_t
{
	hid_t dest_loc;
	hid_t dest_curr_group;
	hid_t dxpl;
};

// local prototypes
static herr_t copy_dataset(hid_t set, hid_t dst_loc, const gchar* dst_name, hid_t dxpl_id);
static herr_t handle_copy(hid_t object, const gchar* name, JHDF5CopyParam_t* copy_data);
static herr_t iterate_copy(hid_t group, const char* name, const H5L_info2_t* info, void* op_data);


static gchar**
parse_vol_path(const gchar* path)
{
	gchar** split_name = NULL;

	g_return_val_if_fail(path != NULL, NULL);
	// string should not be empty (s.t. at least one token is produced)
	g_return_val_if_fail(*path, NULL);

	// assume name to be of the form [<plugin name>://]pathname
	split_name = g_strsplit(path, "://", 2);

	// if the optional part is empty assume native VOL
	if (split_name[1] == NULL)
	{
		g_strfreev(split_name);
		
		split_name = g_malloc(3*sizeof(gchar*));
		split_name[0] = g_strdup("native");
		split_name[1] = g_strdup(path);
		split_name[2] = NULL;
		return split_name;
	
	}

	return split_name;
}

static hid_t
create_fapl_for_vol(const gchar* vol)
{
	hid_t connector_id, fapl;
	if ((connector_id = H5VLregister_connector_by_name(vol, H5P_DEFAULT)) == H5I_INVALID_HID)
	{
		g_printerr("[ERROR] in %s: Error while openening connector with name \"%s\"!\n", G_STRLOC, vol);
		return H5I_INVALID_HID;
	}

	if ((fapl = H5Pcopy(H5P_FILE_ACCESS_DEFAULT)) == H5I_INVALID_HID)
	{
		g_printerr("[ERROR] in %s: Error while creating file access property list!\n", G_STRLOC);
		return H5I_INVALID_HID;
	};

	if (H5Pset_vol(fapl, connector_id, NULL) < 0)
	{
		g_printerr("[ERROR] in %s: Error while setting VOL connector on file access property list.\n", G_STRLOC);
		return H5I_INVALID_HID;
	}

	return fapl;
}

static herr_t
copy_attribute(hid_t location_id, const char *attr_name, const H5A_info_t *ainfo, void *op_data)
{
	herr_t ret = -1;
	hid_t attr, space, type, acpl, new_attr;
	hid_t dst = *((hid_t*)op_data);
	size_t size, count;
	gchar* buf = NULL;

	(void) ainfo;

	if ((attr = H5Aopen(location_id, attr_name, H5P_DEFAULT)) == H5I_INVALID_HID)
	{
		return ret;
	}

	if ((space = H5Aget_space(attr)) == H5I_INVALID_HID)
	{
		goto error_s;
	}

	if ((count = H5Sget_select_npoints(space)) == 0)
	{
		goto error_tp;
	}

	if ((type = H5Aget_type(attr)) == H5I_INVALID_HID)
	{
		goto error_tp;
	}

	if ((size = H5Tget_size(type)) == 0)
	{
		goto error_p;
	}

	if ((acpl = H5Aget_create_plist(attr)) == H5I_INVALID_HID)
	{
		goto error_p;
	}

	if ((new_attr = H5Acreate(dst, attr_name, type, space, acpl, H5P_DEFAULT)) == H5I_INVALID_HID)
	{
		goto error_na;
	}

	if (!(buf = g_malloc(size * count)))
	{
		goto error;
	}

	if (H5Aread(attr, type, buf) < 0)
	{
		goto error;
	}

	if (H5Awrite(new_attr, type, buf) < 0)
	{
		goto error;
	}

	ret = 0;

error:
	H5Aclose(new_attr);
error_na:
	H5Pclose(acpl);
error_p:
	H5Tclose(type);
error_tp:
	H5Sclose(space);
error_s:
	H5Aclose(attr);

	return ret;
}

static herr_t
copy_attributes(hid_t src, hid_t dst)
{
	return H5Aiterate(src, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, copy_attribute, &dst);
}

static herr_t
copy_dataset(hid_t set, hid_t dst_loc, const gchar* dst_name, hid_t dxpl_id)
{
	hid_t dst, space, dtype, dapl;
	g_autofree gchar* buf = NULL;
	size_t mem_size;
	herr_t ret = -1;

	// copy general attributes of the set
	if ((space = H5Dget_space(set)) == H5I_INVALID_HID)
	{
		return ret;
	}

	if ((dtype = H5Dget_type(set)) == H5I_INVALID_HID)
	{
		goto error_tp;
	}

	if ((dapl = H5Dget_access_plist(set)) == H5I_INVALID_HID)
	{
		goto error_p;
	}

	// determine size of data set in memory
	if ((mem_size = H5Sget_select_npoints(space) * H5Tget_size(dtype)) <= 0)
	{
		goto error_d;
	}

	if (!(buf = g_malloc(mem_size)))
	{
		goto error_d;
	}

	if ((dst = H5Dcreate2(dst_loc, dst_name, dtype, space, H5P_DEFAULT, H5P_DEFAULT, dapl)) == H5I_INVALID_HID)
	{
		goto error_d;
	}

	if (copy_attributes(set, dst) < 0)
	{
		goto error;
	}


	if (H5Dread(set, dtype, space, space, dxpl_id, buf) < 0)
	{
		goto error;
	}

	if (H5Dwrite(dst,dtype, space, space, dxpl_id, buf) < 0)
	{
		goto error;
	}

	ret = 0;

error:
	H5Dclose(dst);
error_d:
	H5Pclose(dapl);
error_p:
	H5Tclose(dtype);
error_tp:
	H5Sclose(space);
	return ret;
}

/**
 * \brief Handle copying of an object.
 * 
 * \return A negative number on error.
 **/
static herr_t
handle_copy(hid_t object, const gchar* name, JHDF5CopyParam_t* copy_data)
{
	hid_t tmp_grp;
	herr_t ret = -1;

	switch(H5Iget_type(object))
	{
		case H5I_GROUP:
			tmp_grp = copy_data->dest_curr_group;
			if ((copy_data->dest_curr_group = H5Gcreate(tmp_grp, name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) != H5I_INVALID_HID)
			{
				ret = H5Literate(object, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, iterate_copy, copy_data);
				copy_attributes(object, copy_data->dest_curr_group);
				H5Gclose(copy_data->dest_curr_group);
				copy_data->dest_curr_group = tmp_grp;
			}
			break;

		case H5I_DATASET:
			ret = copy_dataset(object, copy_data->dest_curr_group, name, copy_data->dxpl);
			break;

		case H5I_DATATYPE:
			/// \todo This could be encountered as well but is not (yet) supported by JULEA. Print error and skip.
			g_printerr("%s: Skipped committed datatype while copying!", G_STRLOC);
			ret = 0;
			break;

		case H5I_FILE:
		case H5I_DATASPACE:
		case H5I_ATTR:
		case H5I_UNINIT:
		case H5I_BADID:
		case H5I_MAP:  
		case H5I_VFL:       
		case H5I_VOL:       
		case H5I_GENPROP_CLS:
		case H5I_GENPROP_LST:
		case H5I_ERROR_CLASS:
		case H5I_ERROR_MSG:
		case H5I_ERROR_STACK:
		case H5I_SPACE_SEL_ITER:
		case H5I_NTYPES:
		default:
			g_printerr("%s: Encountered an unexpected object while copying!", G_STRLOC);
			break;
	}
	return ret;
}

/**
 * \brief Iteration operation. Encountered objects are recursively copied.
 * 
 * \return A negative number on error.
 **/
static herr_t
iterate_copy(hid_t group, const char* name, const H5L_info2_t* info, void* op_data)
{
	hid_t src_obj;
	herr_t ret = -1;

	(void) info;

	if ((src_obj = H5Oopen(group, name, H5P_DEFAULT)) == H5I_INVALID_HID)
	{
		return ret;
	}

	ret = handle_copy(src_obj, name, op_data);

	H5Oclose(src_obj);
	
	return ret;
}

static herr_t 
copy_file(hid_t src_file, hid_t dest_file)
{
    JHDF5CopyParam_t copy_data;
	herr_t ret = -1;
	
	copy_data.dest_curr_group = dest_file;
	copy_data.dxpl = H5P_DEFAULT;
	ret = H5Literate(src_file, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, iterate_copy, &copy_data);

	return ret;
}

static void
usage(void)
{
	printf("usage: h5ie [volname://]source [volname://]target\n");
}

int
main(int argc, char** argv)
{
    hid_t src, dst, fapl_vol_src, fapl_vol_dst;
    gchar* source = NULL;
    gchar* destination = NULL;
	gchar** src_components = NULL;
	gchar** dst_components = NULL;
    herr_t ret = -1;

    if(argc <= 2)
    {
		usage();
		return -1;
    }

    /// \todo add options (e.g. chunked copy or copy only some objects like h5copy)
	source = argv[1];
	destination = argv[2];
	printf("Copying %s to %s!\n", source, destination);

    src_components = parse_vol_path(source);
    dst_components = parse_vol_path(destination);

	fapl_vol_src = create_fapl_for_vol(src_components[0]);
	fapl_vol_dst = create_fapl_for_vol(dst_components[0]);

    src = H5Fopen(src_components[1], H5F_ACC_RDONLY, fapl_vol_src);

    dst = H5Fcreate(dst_components[1], H5F_ACC_TRUNC, H5P_DEFAULT, fapl_vol_dst);

    ret = copy_file(src, dst);

	g_strfreev(src_components);
	g_strfreev(dst_components);

	return ret;
}
