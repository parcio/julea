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

#include <glib.h>

#include <julea.h>

#include "benchmark.h"

gchar* opt_path = NULL;
gchar* opt_semantics = NULL;

GTimer* j_benchmark_timer = NULL;

static
JSemantics*
j_benchmark_get_semantics (void)
{
	JSemantics* semantics;
	gchar** parts;
	guint parts_len;

	semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
	parts = g_strsplit(opt_semantics, ",", 0);
	parts_len = g_strv_length(parts);

	for (guint i = 0; i < parts_len; i++)
	{
		if (g_str_has_prefix(parts[i], "consistency:"))
		{

		}
		else if (g_str_has_prefix(parts[i], "persistency:"))
		{

		}
		else if (g_str_has_prefix(parts[i], "concurrency:"))
		{

		}
		else if (g_str_has_prefix(parts[i], "redundancy:"))
		{

		}
		else if (g_str_has_prefix(parts[i], "security:"))
		{

		}
	}

	g_strfreev(parts);

	return semantics;
}

void
j_benchmark_timer_start (void)
{
	g_timer_start(j_benchmark_timer);
}

gdouble
j_benchmark_timer_elapsed (void)
{
	return g_timer_elapsed(j_benchmark_timer, NULL);
}

void
j_benchmark_run (gchar const* name, BenchmarkFunc benchmark_func)
{
	gchar* left;
	gchar* ret;

	if (opt_path != NULL && !g_str_has_prefix(name, opt_path))
	{
		return;
	}

	left = g_strconcat(name, ":", NULL);
	g_print("%-60s ", left);
	g_free(left);

	ret = (*benchmark_func)();

	g_print("%s\n", ret);
	g_free(ret);
}

int
main (int argc, char** argv)
{
	JSemantics* semantics;
	GError* error = NULL;
	GOptionContext* context;

	GOptionEntry entries[] = {
		{ "path", 'p', 0, G_OPTION_ARG_STRING, &opt_path, "Path to use", "/" },
		{ "semantics", 's', 0, G_OPTION_ARG_STRING, &opt_semantics, "Semantics to use", NULL },
		{ NULL }
	};

	context = g_option_context_new(NULL);
	g_option_context_add_main_entries(context, entries, NULL);

	if (!g_option_context_parse(context, &argc, &argv, &error))
	{
		g_option_context_free(context);

		if (error)
		{
			g_printerr("%s\n", error->message);
			g_error_free(error);
		}

		return 1;
	}

	g_option_context_free(context);

	j_init(&argc, &argv);

	semantics = j_benchmark_get_semantics();
	j_benchmark_timer = g_timer_new();

	benchmark_background_operation();
	benchmark_cache();
	benchmark_collection();
	benchmark_item();

	g_timer_destroy(j_benchmark_timer);
	j_semantics_unref(semantics);

	j_fini();

	g_free(opt_path);
	g_free(opt_semantics);

	return 0;
}
