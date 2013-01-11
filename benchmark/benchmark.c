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

#include "benchmark.h"

gchar* opt_path = NULL;
gchar* opt_semantics = NULL;
gchar* opt_template = NULL;

GTimer* j_benchmark_timer = NULL;

JSemantics*
j_benchmark_get_semantics (void)
{
	JSemantics* semantics;
	gchar** parts;
	guint parts_len;

	if (g_strcmp0(opt_template, "posix") == 0)
	{
		semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_POSIX);
	}
	else if (g_strcmp0(opt_template, "checkpoint") == 0)
	{
		semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_CHECKPOINT);
	}
	else
	{
		semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
	}

	if (opt_semantics == NULL)
	{
		return semantics;
	}

	parts = g_strsplit(opt_semantics, ",", 0);
	parts_len = g_strv_length(parts);

	for (guint i = 0; i < parts_len; i++)
	{
		gchar const* value;

		if ((value = strchr(parts[i], '=')) == NULL)
		{
			continue;
		}

		value++;

		if (g_str_has_prefix(parts[i], "atomicity="))
		{
			if (g_strcmp0(value, "operation") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_ATOMICITY, J_SEMANTICS_ATOMICITY_OPERATION);
			}
			else if (g_strcmp0(value, "sub-operation") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_ATOMICITY, J_SEMANTICS_ATOMICITY_SUB_OPERATION);
			}
			else if (g_strcmp0(value, "none") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_ATOMICITY, J_SEMANTICS_ATOMICITY_NONE);
			}
		}
		else if (g_str_has_prefix(parts[i], "concurrency="))
		{
			if (g_strcmp0(value, "overlapping") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_CONCURRENCY, J_SEMANTICS_CONCURRENCY_OVERLAPPING);
			}
			else if (g_strcmp0(value, "non-overlapping") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_CONCURRENCY, J_SEMANTICS_CONCURRENCY_NON_OVERLAPPING);
			}
			else if (g_strcmp0(value, "none") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_CONCURRENCY, J_SEMANTICS_CONCURRENCY_NONE);
			}
		}
		else if (g_str_has_prefix(parts[i], "consistency="))
		{
			if (g_strcmp0(value, "immediate") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_CONSISTENCY, J_SEMANTICS_CONSISTENCY_IMMEDIATE);
			}
			else if (g_strcmp0(value, "eventual") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_CONSISTENCY, J_SEMANTICS_CONSISTENCY_EVENTUAL);
			}
			else if (g_strcmp0(value, "none") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_CONSISTENCY, J_SEMANTICS_CONSISTENCY_NONE);
			}
		}
		else if (g_str_has_prefix(parts[i], "persistency="))
		{
			if (g_strcmp0(value, "immediate") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_PERSISTENCY, J_SEMANTICS_PERSISTENCY_IMMEDIATE);
			}
			else if (g_strcmp0(value, "eventual") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_PERSISTENCY, J_SEMANTICS_PERSISTENCY_EVENTUAL);
			}
			else if (g_strcmp0(value, "none") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_PERSISTENCY, J_SEMANTICS_PERSISTENCY_NONE);
			}
		}
		else if (g_str_has_prefix(parts[i], "safety="))
		{
			if (g_strcmp0(value, "storage") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_SAFETY, J_SEMANTICS_SAFETY_STORAGE);
			}
			else if (g_strcmp0(value, "network") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_SAFETY, J_SEMANTICS_SAFETY_NETWORK);
			}
			else if (g_strcmp0(value, "none") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_SAFETY, J_SEMANTICS_SAFETY_NONE);
			}
		}
		else if (g_str_has_prefix(parts[i], "security="))
		{
			if (g_strcmp0(value, "strict") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_SECURITY, J_SEMANTICS_SECURITY_STRICT);
			}
			else if (g_strcmp0(value, "none") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_SECURITY, J_SEMANTICS_SECURITY_NONE);
			}
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
	JSemantics* semantics;
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

	semantics = j_benchmark_get_semantics();
	j_benchmark_timer = g_timer_new();

	benchmark_background_operation();
	benchmark_cache();
	benchmark_collection();
	benchmark_item();
	benchmark_lock();
	benchmark_message();

	g_timer_destroy(j_benchmark_timer);
	j_semantics_unref(semantics);

	j_fini();

	g_free(opt_path);
	g_free(opt_semantics);
	g_free(opt_template);

	return 0;
}
