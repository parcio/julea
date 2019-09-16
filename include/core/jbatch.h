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

#ifndef JULEA_BATCH_H
#define JULEA_BATCH_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

struct JBatch;

typedef struct JBatch JBatch;

typedef void (*JBatchAsyncCallback) (JBatch*, gboolean, gpointer);

G_END_DECLS

#include <core/joperation.h>
#include <core/jsemantics.h>

G_BEGIN_DECLS

JBatch* j_batch_new (JSemantics*);
JBatch* j_batch_new_for_template (JSemanticsTemplate);
JBatch* j_batch_ref (JBatch*);
void j_batch_unref (JBatch*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JBatch, j_batch_unref)

JSemantics* j_batch_get_semantics (JBatch*);

void j_batch_add (JBatch*, JOperation*);

gboolean j_batch_execute (JBatch*) G_GNUC_WARN_UNUSED_RESULT;

void j_batch_execute_async (JBatch*, JBatchAsyncCallback, gpointer);
void j_batch_wait (JBatch*);

G_END_DECLS

#endif
