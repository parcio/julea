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

#ifndef JULEA_SEMANTICS_H
#define JULEA_SEMANTICS_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

enum JSemanticsTemplate
{
	J_SEMANTICS_TEMPLATE_DEFAULT,
	J_SEMANTICS_TEMPLATE_POSIX,
	J_SEMANTICS_TEMPLATE_TEMPORARY_LOCAL
};

typedef enum JSemanticsTemplate JSemanticsTemplate;

enum JSemanticsType
{
	J_SEMANTICS_ATOMICITY,
	J_SEMANTICS_CONCURRENCY,
	J_SEMANTICS_CONSISTENCY,
	J_SEMANTICS_ORDERING,
	J_SEMANTICS_PERSISTENCY,
	J_SEMANTICS_SAFETY,
	J_SEMANTICS_SECURITY
};

typedef enum JSemanticsType JSemanticsType;

enum JSemanticsAtomicity
{
	J_SEMANTICS_ATOMICITY_BATCH,
	J_SEMANTICS_ATOMICITY_OPERATION,
	J_SEMANTICS_ATOMICITY_NONE
};

typedef enum JSemanticsAtomicity JSemanticsAtomicity;

enum JSemanticsConcurrency
{
	J_SEMANTICS_CONCURRENCY_OVERLAPPING,
	J_SEMANTICS_CONCURRENCY_NON_OVERLAPPING,
	J_SEMANTICS_CONCURRENCY_NONE
};

typedef enum JSemanticsConcurrency JSemanticsConcurrency;

enum JSemanticsConsistency
{
	J_SEMANTICS_CONSISTENCY_IMMEDIATE,
	J_SEMANTICS_CONSISTENCY_EVENTUAL,
	J_SEMANTICS_CONSISTENCY_NONE
};

typedef enum JSemanticsConsistency JSemanticsConsistency;

enum JSemanticsOrdering
{
	J_SEMANTICS_ORDERING_STRICT,
	J_SEMANTICS_ORDERING_SEMI_RELAXED,
	J_SEMANTICS_ORDERING_RELAXED
};

typedef enum JSemanticsOrdering JSemanticsOrdering;

enum JSemanticsPersistency
{
	J_SEMANTICS_PERSISTENCY_IMMEDIATE,
	J_SEMANTICS_PERSISTENCY_EVENTUAL,
	J_SEMANTICS_PERSISTENCY_NONE
};

typedef enum JSemanticsPersistency JSemanticsPersistency;

enum JSemanticsSafety
{
	J_SEMANTICS_SAFETY_STORAGE,
	J_SEMANTICS_SAFETY_NETWORK,
	J_SEMANTICS_SAFETY_NONE
};

typedef enum JSemanticsSafety JSemanticsSafety;

enum JSemanticsSecurity
{
	J_SEMANTICS_SECURITY_STRICT,
	J_SEMANTICS_SECURITY_NONE
};

typedef enum JSemanticsSecurity JSemanticsSecurity;

struct JSemantics;

typedef struct JSemantics JSemantics;

JSemantics* j_semantics_new (JSemanticsTemplate);
JSemantics* j_semantics_new_from_string (gchar const*, gchar const*);

JSemantics* j_semantics_ref (JSemantics*);
void j_semantics_unref (JSemantics*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JSemantics, j_semantics_unref)

void j_semantics_set (JSemantics*, JSemanticsType, gint);
gint j_semantics_get (JSemantics*, JSemanticsType);

G_END_DECLS

#endif
