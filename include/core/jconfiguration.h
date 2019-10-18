/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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

#ifndef JULEA_CONFIGURATION_H
#define JULEA_CONFIGURATION_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

#include <core/jbackend.h>

G_BEGIN_DECLS

struct JConfiguration;

typedef struct JConfiguration JConfiguration;

JConfiguration* j_configuration (void);

JConfiguration* j_configuration_new (void);
JConfiguration* j_configuration_new_for_data (GKeyFile*);

JConfiguration* j_configuration_ref (JConfiguration*);
void j_configuration_unref (JConfiguration*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JConfiguration, j_configuration_unref)

gchar const* j_configuration_get_server (JConfiguration*, JBackendType, guint32);
guint32 j_configuration_get_server_count (JConfiguration*, JBackendType);

gchar const* j_configuration_get_backend (JConfiguration*, JBackendType);
gchar const* j_configuration_get_backend_component (JConfiguration*, JBackendType);
gchar const* j_configuration_get_backend_path (JConfiguration*, JBackendType);

guint64 j_configuration_get_max_operation_size (JConfiguration*);
guint32 j_configuration_get_max_connections (JConfiguration*);
guint64 j_configuration_get_stripe_size (JConfiguration*);

G_END_DECLS

#endif
