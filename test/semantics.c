/*
 * Copyright (c) 2010-2012 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>

#include <julea.h>

#include "test.h"

static
void
test_semantics_fixture_setup (JSemantics** semantics, G_GNUC_UNUSED gconstpointer data)
{
	*semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
}

static
void
test_semantics_fixture_teardown (JSemantics** semantics, G_GNUC_UNUSED gconstpointer data)
{
	j_semantics_unref(*semantics);
}

static
void
test_semantics_new_ref_unref (void)
{
	guint const n = 100000;

	for (guint i = 0; i < n; i++)
	{
		JSemantics* semantics;

		semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
		g_assert(semantics != NULL);
		semantics = j_semantics_ref(semantics);
		g_assert(semantics != NULL);
		j_semantics_unref(semantics);
		j_semantics_unref(semantics);
	}
}

static
void
test_semantics_set_get (JSemantics** semantics, G_GNUC_UNUSED gconstpointer data)
{
	gint s;

	j_semantics_set(*semantics, J_SEMANTICS_ATOMICITY, J_SEMANTICS_ATOMICITY_SUB_OPERATION);
	s = j_semantics_get(*semantics, J_SEMANTICS_ATOMICITY);
	g_assert_cmpint(s, ==, J_SEMANTICS_ATOMICITY_SUB_OPERATION);

	j_semantics_set(*semantics, J_SEMANTICS_CONCURRENCY, J_SEMANTICS_CONCURRENCY_OVERLAPPING);
	s = j_semantics_get(*semantics, J_SEMANTICS_CONCURRENCY);
	g_assert_cmpint(s, ==, J_SEMANTICS_CONCURRENCY_OVERLAPPING);

	j_semantics_set(*semantics, J_SEMANTICS_CONSISTENCY, J_SEMANTICS_CONSISTENCY_IMMEDIATE);
	s = j_semantics_get(*semantics, J_SEMANTICS_CONSISTENCY);
	g_assert_cmpint(s, ==, J_SEMANTICS_CONSISTENCY_IMMEDIATE);

	j_semantics_set(*semantics, J_SEMANTICS_PERSISTENCY, J_SEMANTICS_PERSISTENCY_EVENTUAL);
	s = j_semantics_get(*semantics, J_SEMANTICS_PERSISTENCY);
	g_assert_cmpint(s, ==, J_SEMANTICS_PERSISTENCY_EVENTUAL);

	j_semantics_set(*semantics, J_SEMANTICS_SAFETY, J_SEMANTICS_SAFETY_HIGH);
	s = j_semantics_get(*semantics, J_SEMANTICS_SAFETY);
	g_assert_cmpint(s, ==, J_SEMANTICS_SAFETY_HIGH);

	j_semantics_set(*semantics, J_SEMANTICS_SECURITY, J_SEMANTICS_SECURITY_STRICT);
	s = j_semantics_get(*semantics, J_SEMANTICS_SECURITY);
	g_assert_cmpint(s, ==, J_SEMANTICS_SECURITY_STRICT);
}

void
test_semantics (void)
{
	g_test_add_func("/semantics/new_ref_unref", test_semantics_new_ref_unref);
	g_test_add("/semantics/set_get", JSemantics*, NULL, test_semantics_fixture_setup, test_semantics_set_get, test_semantics_fixture_teardown);
}
