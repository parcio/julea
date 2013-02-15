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

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <julea.h>

#include <jhelper-internal.h>

#include "benchmark.h"

gchar* opt_path = NULL;
gchar* opt_semantics = NULL;
gchar* opt_template = NULL;

JSemantics* j_benchmark_semantics = NULL;

GTimer* j_benchmark_timer = NULL;

JSemantics*
j_benchmark_get_semantics (void)
{
	return j_semantics_ref(j_benchmark_semantics);
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
	GTimer* func_timer;
	gchar* left;
	gchar* ret;
	gdouble elapsed;

	if (opt_path != NULL && !g_str_has_prefix(name, opt_path))
	{
		return;
	}

	func_timer = g_timer_new();

	left = g_strconcat(name, ":", NULL);
	g_print("%-60s ", left);
	g_free(left);

	g_timer_start(func_timer);
	ret = (*benchmark_func)();
	elapsed = g_timer_elapsed(func_timer, NULL);

	g_print("%s [%f seconds]\n", ret, elapsed);
	g_free(ret);

	g_timer_destroy(func_timer);
}

int
main (int argc, char** argv)
{
	GError* error = NULL;
	GOptionContext* context;

	GOptionEntry entries[] = {
		{ "path", 'p', 0, G_OPTION_ARG_STRING, &opt_path, "Benchmark path to use", "/" },
		{ "semantics", 's', 0, G_OPTION_ARG_STRING, &opt_semantics, "Semantics to use", NULL },
		{ "template", 't', 0, G_OPTION_ARG_STRING, &opt_template, "Semantics template to use", "default" },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
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

	j_benchmark_semantics = j_helper_parse_semantics(opt_template, opt_semantics);
	j_benchmark_timer = g_timer_new();

	benchmark_background_operation();
	benchmark_cache();
	benchmark_collection();
	benchmark_item();
	benchmark_lock();
	benchmark_message();

	g_timer_destroy(j_benchmark_timer);
	j_semantics_unref(j_benchmark_semantics);

	j_fini();

	g_free(opt_path);
	g_free(opt_semantics);
	g_free(opt_template);

	return 0;
}
