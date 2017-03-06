/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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

#ifndef H_CONFIGURATION_INTERNAL
#define H_CONFIGURATION_INTERNAL

#include <glib.h>

#include <julea-internal.h>

struct JConfiguration;

typedef struct JConfiguration JConfiguration;

J_GNUC_INTERNAL JConfiguration* j_configuration_new (void);
J_GNUC_INTERNAL JConfiguration* j_configuration_new_for_data (GKeyFile*);

J_GNUC_INTERNAL JConfiguration* j_configuration_ref (JConfiguration*);
J_GNUC_INTERNAL void j_configuration_unref (JConfiguration*);

J_GNUC_INTERNAL gchar const* j_configuration_get_data_server (JConfiguration*, guint);
J_GNUC_INTERNAL gchar const* j_configuration_get_metadata_server (JConfiguration*, guint);

J_GNUC_INTERNAL guint j_configuration_get_data_server_count (JConfiguration*);
J_GNUC_INTERNAL guint j_configuration_get_metadata_server_count (JConfiguration*);

J_GNUC_INTERNAL gchar const* j_configuration_get_data_backend (JConfiguration*);
J_GNUC_INTERNAL gchar const* j_configuration_get_data_path (JConfiguration*);

J_GNUC_INTERNAL gchar const* j_configuration_get_metadata_backend (JConfiguration*);
J_GNUC_INTERNAL gchar const* j_configuration_get_metadata_path (JConfiguration*);

J_GNUC_INTERNAL guint j_configuration_get_max_connections (JConfiguration*);

#endif
