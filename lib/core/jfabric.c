/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
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

#include <julea-config.h>

#include <julea.h>

#include <jfabric.h>

struct JFabric
{
	struct fid_fabric* msg_fabric;
	struct fi_info* msg_info;
	struct fid_fabric* rdma_fabric;
	struct fi_info* rdma_info;
};

gboolean
j_fabric_init(JFabric** jfabric, JRequestType req_type, JConfiguration* configuration, const gchar* location)
{
	gboolean ret;
	int error;

	J_TRACE_FUNCTION(NULL);
	g_return_val_if_fail(configuration != NULL, FALSE);

	ret = FALSE;
	error = 0;

	*jfabric = malloc(sizeof(struct JFabric));

	//get fabric fi_info
	error = fi_getinfo(j_configuration_get_fi_version(configuration),
			   NULL,
			   j_configuration_get_fi_service(configuration),
			   j_configuration_get_fi_flags(configuration, req_type),
			   j_configuration_fi_get_hints(configuration, J_MSG),
			   &(*jfabric)->msg_info);
	if (error != 0)
	{
		g_critical("\n%s: Error while initiating fi_info for msg fabric.\n Details:\n %s", location, fi_strerror(abs(error)));
		goto end;
	}
	if ((*jfabric)->msg_info == NULL)
	{
		g_critical("\n%s: Allocating msg fi_info struct did not work", location);
		goto end;
	}

	error = fi_getinfo(j_configuration_get_fi_version(configuration),
			   NULL,
			   j_configuration_get_fi_service(configuration),
			   j_configuration_get_fi_flags(configuration, req_type),
			   j_configuration_fi_get_hints(configuration, J_RDMA),
			   &(*jfabric)->rdma_info);
	if (error != 0)
	{
		g_critical("\n%s: Error while initiating fi_info for rdma fabric.\n Details:\n %s", location, fi_strerror(abs(error)));
		goto end;
	}
	if ((*jfabric)->rdma_info == NULL)
	{
		g_critical("\n%s: Allocating rdma fi_info struct did not work", location);
		goto end;
	}

	//Init fabric
	error = fi_fabric((*jfabric)->msg_info->fabric_attr, &(*jfabric)->msg_fabric, NULL);
	if (error != FI_SUCCESS)
	{
		g_critical("\n%s: Error during initializing msg fabric\n Details:\n %s", location, fi_strerror(abs(error)));
		goto end;
	}
	if ((*jfabric)->msg_fabric == NULL)
	{
		g_critical("\n%s: Allocating msg fabric did not work", location);
		goto end;
	}

	error = fi_fabric((*jfabric)->rdma_info->fabric_attr, &(*jfabric)->rdma_fabric, NULL);
	if (error != FI_SUCCESS)
	{
		g_critical("\n%s: Error during initializing rdma fabric\n Details:\n %s", location, fi_strerror(abs(error)));
		goto end;
	}
	if ((*jfabric)->msg_fabric == NULL)
	{
		g_critical("\n%s: Allocating rdma fabric did not work", location);
		goto end;
	}

	ret = TRUE;
end:
	return ret;
}

gboolean
j_fabric_fini(JFabric* jfabric, const gchar* location)
{
	gboolean ret;
	int error;

	J_TRACE_FUNCTION(NULL);
	g_return_val_if_fail(jfabric != NULL, FALSE);

	ret = FALSE;
	error = 0;

	error = fi_close(&jfabric->msg_fabric->fid);
	if (error != 0)
	{
		g_critical("\n%s: Error closing msg fi_fabric.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}
	error = fi_close(&jfabric->rdma_fabric->fid);
	if (error != 0)
	{
		g_critical("\n%s: Error closing rdma fi_fabric.\n Details:\n %s", location, fi_strerror(abs(error)));
		error = 0;
	}

	fi_freeinfo(jfabric->msg_info);
	fi_freeinfo(jfabric->rdma_info);
	free(jfabric);

	ret = TRUE;
	return ret;
}

struct fid_fabric*
j_get_fabric(JFabric* jfabric, JConnectionType con_type)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(jfabric != NULL, NULL);

	switch (con_type)
	{
		case J_MSG:
			return jfabric->msg_fabric;
		case J_RDMA:
			return jfabric->rdma_fabric;
		case J_UNDEFINED:
		default:
			g_assert_not_reached();
	}
}

struct fi_info*
j_get_info(JFabric* jfabric, JConnectionType con_type)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(jfabric != NULL, NULL);

	switch (con_type)
	{
		case J_MSG:
			return jfabric->msg_info;
		case J_RDMA:
			return jfabric->rdma_info;
		case J_UNDEFINED:
		default:
			g_assert_not_reached();
	}
}
