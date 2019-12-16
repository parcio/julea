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

#include <julea.h>
#include <julea-object.h>
#include <julea-kv.h>
#include <julea-item.h>

#include <glib.h>

void j_cmd_usage(void);

guint j_cmd_arguments_length(gchar const**);

gboolean j_cmd_error_last(JURI*);

gboolean j_cmd_create(gchar const**, gboolean);
gboolean j_cmd_copy(gchar const**);
gboolean j_cmd_delete(gchar const**);
gboolean j_cmd_list(gchar const**);
gboolean j_cmd_status(gchar const**);
