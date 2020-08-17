/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
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

#ifndef JULEA_FABRIC_H
#define JULEA_FABRIC_H

#include <glib.h>
#include <gio/gio.h>

#include <jstatistics.h>
#include <jconfiguration.h>

#include <rdma/fabric.h>
#include <rdma/fi_errno.h> //translation error numbers

struct JFabric;
typedef struct JFabric JFabric;


gboolean j_fabric_init(JFabric**, JRequestType, JConfiguration*, const gchar*);
gboolean j_fabric_fini(JFabric*, const gchar*);

struct fid_fabric* j_get_fabric(JFabric*, JConnectionType);
struct fi_info* j_get_info(JFabric*, JConnectionType);


#endif
