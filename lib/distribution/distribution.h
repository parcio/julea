/*
 * Copyright (c) 2010-2013 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/**
 * \file
 **/

#ifndef H_DISTRIBUTION_DISTRIBUTION
#define H_DISTRIBUTION_DISTRIBUTION

#include <glib.h>

#include <bson.h>

#include <jconfiguration-internal.h>

struct JDistributionVTable
{
	gpointer (*distribution_new) (guint);
	void (*distribution_free) (gpointer);

	void (*distribution_set) (gpointer, gchar const*, guint64);
	void (*distribution_set2) (gpointer, gchar const*, guint64, guint64);

	void (*distribution_serialize) (gpointer, bson*);
	void (*distribution_deserialize) (gpointer, bson const*);

	void (*distribution_reset) (gpointer, guint64, guint64);
	gboolean (*distribution_distribute) (gpointer, guint*, guint64*, guint64*, guint64*);
};

typedef struct JDistributionVTable JDistributionVTable;

void j_distribution_round_robin_get_vtable (JDistributionVTable*);
void j_distribution_single_server_get_vtable (JDistributionVTable*);
void j_distribution_weighted_get_vtable (JDistributionVTable*);

#endif
