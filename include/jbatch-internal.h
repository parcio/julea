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

#ifndef H_BATCH_INTERNAL
#define H_BATCH_INTERNAL

#include <glib.h>

#include <julea-internal.h>

#include <jbatch.h>

#include <joperation-internal.h>

J_GNUC_INTERNAL JBatch* j_batch_new_from_batch (JBatch*);

J_GNUC_INTERNAL JList* j_batch_get_operations (JBatch*);
J_GNUC_INTERNAL JSemantics* j_batch_get_semantics (JBatch*);

J_GNUC_INTERNAL void j_batch_add (JBatch*, JOperation*);

J_GNUC_INTERNAL gboolean j_batch_execute_internal (JBatch*);

#endif
