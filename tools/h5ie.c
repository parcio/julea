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
	hid_t dest_curr_group;
	hid_t dxpl;
};

/**
 * Cleanup function for hid_t.
 *
 * Wraps H5Idec_ref() which can not handle H5I_INVALID_HID (or other unassigned HDF5 handles).
 *
 * \param obj The object to free.
 *
 */
static void hid_cleanup(hid_t* obj);

/**
 * Parse the path of a HDF5 file.
 *
 * The path may be prefixed by the name of the corresponding VOL plugin followed by "://".
 * Returns a null terminated GStrv with 2 entries. The first one contains the VOL name and the second one the rest of the path.
 * If no VOL name was given the first entry is set to "native".
 *
 * \param path The Path to a HDF5 file (possibly managed by a VOL plugin).
 *
 * \return A GStrv as described above or NULL in case of invalid input. Should be freed using g_strfreev().
 */
static gchar** parse_vol_path(const gchar* path);

/**
 * Creates a file access property list using the given VOL name.
 *
 * \param vol The name of the VOL plugin.
 *
 * \return The ID of the new file access property list or H5I_INVALID_HID in case of error.
 */
static hid_t create_fapl_for_vol(const gchar* vol);

/**
 * Copy the files content to a different file.
 *
 * \param src_file The source file.
 * \param dst_file The destination file.
 *
 * \return A negative value on error.
 */
static herr_t copy_file(hid_t src_file, hid_t dest_file);

/**
 * Handle the copy of an object of unknown type.
 *
 * \param object The object to copy.
 * \param dst_name The name of the new copy.
 * \param copy_data Further parameters such as the destination parent
 * 					group and the data transfer property list. More
 * 					parameters (like the block size for copying) might
 * 					be added later.
 *
 * \return A negative value on error.
 */
static herr_t handle_copy(hid_t object, const gchar* name, JHDF5CopyParam_t* copy_data);

/**
 * Copy a dataset to a new location.
 *
 * \param set The dataset to copy.
 * \param dst_loc The location identfier of the target group.
 * \param dst_name The name of the new copy.
 * \param dxpl_id The dataset transfer property list.
 *
 * \return A negative value on error.
 */
static herr_t copy_dataset(hid_t set, hid_t dst_loc, const gchar* dst_name, hid_t dxpl_id);

/**
 * Function of type H5L_iterate2_t. Basically a thin wrapper of handle_copy().
 *
 * This function gets called by H5Literate2().
 *
 * \param group The group currently iterated.
 * \param name The name of the encountered object.
 * \param info Information of the current link.
 * \param op_data Should be of type JHDF5CopyParam_t.
 *
 * \return A negative value on error.
 */
static herr_t iterate_copy(hid_t group, const char* name, const H5L_info2_t* info, void* op_data);

/**
 * Copy an attribute to a new location. Function of type H5A_operator2_t.
 *
 * Gets called by H5Aiterate().
 *
 * \param location_id The object whose attributes are iterated.
 * \param attr_name The name of the current attribute.
 * \param ainfo Information about the current attribute.
 * \param op_data A pointer to the hid_t of the destination.
 *
 * \return A negative value on error.
 */
static herr_t copy_attribute(hid_t location_id, const char* attr_name, const H5A_info_t* ainfo, void* op_data);

/**
 * Copy all attributes of an object to another object.
 *
 * \param src The source object.
 * \param dst The destination object.
 *
 * \return A negative value on error.
 */
static herr_t copy_attributes(hid_t src, hid_t dst);

/**
 * Print the usage.
 */
static void usage(void);

static void
hid_cleanup(hid_t* obj)
{
	g_assert(obj != NULL);

	if (*obj != H5I_INVALID_HID)
	{
		H5Idec_ref(*obj);
	}
}

G_DEFINE_AUTO_CLEANUP_CLEAR_FUNC(hid_t, hid_cleanup)

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

		split_name = g_malloc(3 * sizeof(gchar*));
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
	g_auto(hid_t) connector_id = H5I_INVALID_HID;
	hid_t fapl = H5I_INVALID_HID;

	g_return_val_if_fail(vol != NULL, H5I_INVALID_HID);

	if ((connector_id = H5VLregister_connector_by_name(vol, H5P_DEFAULT)) == H5I_INVALID_HID)
	{
		g_critical("%s: Error while openening connector with name \"%s\"!", G_STRLOC, vol);
		return H5I_INVALID_HID;
	}

	if ((fapl = H5Pcopy(H5P_FILE_ACCESS_DEFAULT)) == H5I_INVALID_HID)
	{
		return H5I_INVALID_HID;
	};

	if (H5Pset_vol(fapl, connector_id, NULL) < 0)
	{
		H5Pclose(fapl);
		return H5I_INVALID_HID;
	}

	return fapl;
}

static herr_t
copy_file(hid_t src_file, hid_t dest_file)
{
	JHDF5CopyParam_t copy_data;
	herr_t retval = -1;

	copy_data.dest_curr_group = dest_file;
	copy_data.dxpl = H5P_DEFAULT;
	retval = H5Literate(src_file, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, iterate_copy, &copy_data);

	return retval;
}

static herr_t
handle_copy(hid_t object, const gchar* name, JHDF5CopyParam_t* copy_data)
{
	hid_t tmp_grp;
	herr_t retval = -1;

	switch (H5Iget_type(object))
	{
		case H5I_GROUP:
			tmp_grp = copy_data->dest_curr_group;
			if ((copy_data->dest_curr_group = H5Gcreate(tmp_grp, name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT)) != H5I_INVALID_HID)
			{
				if ((retval = H5Literate(object, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, iterate_copy, copy_data)) >= 0)
				{
					retval = copy_attributes(object, copy_data->dest_curr_group);
				}

				H5Gclose(copy_data->dest_curr_group);
				copy_data->dest_curr_group = tmp_grp;
			}
			break;

		case H5I_DATASET:
			retval = copy_dataset(object, copy_data->dest_curr_group, name, copy_data->dxpl);
			break;

		case H5I_DATATYPE:
			/// \todo This could be encountered as well but is not (yet) supported by JULEA. Print error and skip.
			g_warning("%s: Skipped committed datatype while copying!", G_STRLOC);
			retval = 0;
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
			g_critical("%s: Encountered an unexpected object while copying!", G_STRLOC);
			break;
	}
	return retval;
}

static herr_t
copy_dataset(hid_t set, hid_t dst_loc, const gchar* dst_name, hid_t dxpl_id)
{
	g_auto(hid_t) dst = H5I_INVALID_HID;
	g_auto(hid_t) space = H5I_INVALID_HID;
	g_auto(hid_t) dtype = H5I_INVALID_HID;
	g_auto(hid_t) dapl = H5I_INVALID_HID;
	g_autofree gchar* buf = NULL;
	size_t mem_size;
	herr_t retval = -1;

	// copy general attributes of the set
	if ((space = H5Dget_space(set)) == H5I_INVALID_HID)
	{
		return retval;
	}

	if ((dtype = H5Dget_type(set)) == H5I_INVALID_HID)
	{
		return retval;
	}

	if ((dapl = H5Dget_access_plist(set)) == H5I_INVALID_HID)
	{
		return retval;
	}

	// determine size of data set in memory
	if ((mem_size = H5Sget_select_npoints(space) * H5Tget_size(dtype)) <= 0)
	{
		return retval;
	}

	if (!(buf = g_malloc(mem_size)))
	{
		return retval;
	}

	if ((dst = H5Dcreate2(dst_loc, dst_name, dtype, space, H5P_DEFAULT, H5P_DEFAULT, dapl)) == H5I_INVALID_HID)
	{
		return retval;
	}

	if (copy_attributes(set, dst) < 0)
	{
		return retval;
	}

	if (H5Dread(set, dtype, space, space, dxpl_id, buf) < 0)
	{
		return retval;
	}

	if (H5Dwrite(dst, dtype, space, space, dxpl_id, buf) < 0)
	{
		return retval;
	}

	retval = 0;
	return retval;
}

static herr_t
iterate_copy(hid_t group, const char* name, const H5L_info2_t* info, void* op_data)
{
	g_auto(hid_t) src_obj = H5I_INVALID_HID;
	herr_t retval = -1;

	(void)info;

	if ((src_obj = H5Oopen(group, name, H5P_DEFAULT)) == H5I_INVALID_HID)
	{
		return retval;
	}

	retval = handle_copy(src_obj, name, op_data);

	return retval;
}

static herr_t
copy_attribute(hid_t location_id, const char* attr_name, const H5A_info_t* ainfo, void* op_data)
{
	g_auto(hid_t) attr = H5I_INVALID_HID;
	g_auto(hid_t) space = H5I_INVALID_HID;
	g_auto(hid_t) type = H5I_INVALID_HID;
	g_auto(hid_t) acpl = H5I_INVALID_HID;
	g_auto(hid_t) new_attr = H5I_INVALID_HID;
	g_autofree gchar* buf = NULL;
	hid_t dst = *((hid_t*)op_data);
	herr_t retval = -1;
	size_t size, count;

	(void)ainfo;

	if ((attr = H5Aopen(location_id, attr_name, H5P_DEFAULT)) == H5I_INVALID_HID)
	{
		return retval;
	}

	if ((space = H5Aget_space(attr)) == H5I_INVALID_HID)
	{
		return retval;
	}

	if ((count = H5Sget_select_npoints(space)) == 0)
	{
		return retval;
	}

	if ((type = H5Aget_type(attr)) == H5I_INVALID_HID)
	{
		return retval;
	}

	if ((size = H5Tget_size(type)) == 0)
	{
		return retval;
	}

	if ((acpl = H5Aget_create_plist(attr)) == H5I_INVALID_HID)
	{
		return retval;
	}

	if ((new_attr = H5Acreate(dst, attr_name, type, space, acpl, H5P_DEFAULT)) == H5I_INVALID_HID)
	{
		return retval;
	}

	if (!(buf = g_malloc(size * count)))
	{
		return retval;
	}

	if (H5Aread(attr, type, buf) < 0)
	{
		return retval;
	}

	if (H5Awrite(new_attr, type, buf) < 0)
	{
		return retval;
	}

	retval = 0;
	return retval;
}

static herr_t
copy_attributes(hid_t src, hid_t dst)
{
	return H5Aiterate(src, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, copy_attribute, &dst);
}

static void
usage(void)
{
	printf("Usage: h5ie [volname://]source [volname://]target\n");
}

int
main(int argc, char** argv)
{
	g_auto(hid_t) src = H5I_INVALID_HID;
	g_auto(hid_t) dst = H5I_INVALID_HID;
	g_auto(hid_t) fapl_vol_src = H5I_INVALID_HID;
	g_auto(hid_t) fapl_vol_dst = H5I_INVALID_HID;
	g_auto(GStrv) src_components = NULL;
	g_auto(GStrv) dst_components = NULL;
	gchar* source = NULL;
	gchar* destination = NULL;
	herr_t retval = -1;

	/**
	 * \todo Needed because otherwise native VOL cannot be used.
	 */
	g_setenv("HDF5_VOL_CONNECTOR", "native", true);

	if (argc <= 2)
	{
		usage();
		return retval;
	}

	/// \todo add options (e.g. chunked copy or copy only some objects like h5copy)
	source = argv[1];
	destination = argv[2];

	// disable long HDF5 error prints
	H5Eset_auto2(H5E_DEFAULT, NULL, NULL);

	if ((src_components = parse_vol_path(source)) == NULL)
	{
		g_critical("%s: Could not parse VOL connector for source file!", G_STRLOC);
		return retval;
	}

	if ((dst_components = parse_vol_path(destination)) == NULL)
	{
		g_critical("%s: Could not parse VOL connector for destination file!", G_STRLOC);
		return retval;
	}

	if ((fapl_vol_src = create_fapl_for_vol(src_components[0])) == H5I_INVALID_HID)
	{
		g_critical("%s: Could not create file access property list with given VOL!", G_STRLOC);
		return retval;
	}

	if ((fapl_vol_dst = create_fapl_for_vol(dst_components[0])) == H5I_INVALID_HID)
	{
		g_critical("%s: Could not create file access property list with given VOL!", G_STRLOC);
		return retval;
	}

	if ((src = H5Fopen(src_components[1], H5F_ACC_RDONLY, fapl_vol_src)) == H5I_INVALID_HID)
	{
		g_critical("%s: Could not open source file!", G_STRLOC);
		return retval;
	}

	if ((dst = H5Fcreate(dst_components[1], H5F_ACC_EXCL, H5P_DEFAULT, fapl_vol_dst)) == H5I_INVALID_HID)
	{
		g_critical("%s: Could not create target file!", G_STRLOC);
		return retval;
	}

	g_info("Copying %s to %s!", source, destination);
	retval = copy_file(src, dst);

	if (retval < 0)
	{
		g_critical("%s: Could not copy all of the data!", G_STRLOC);
	}

	return retval;
}
