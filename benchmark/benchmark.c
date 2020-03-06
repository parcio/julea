/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2020 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <julea.h>

#include "benchmark.h"

static gchar* opt_machine_separator = NULL;
static gboolean opt_machine_readable = FALSE;
static gchar* opt_path = NULL;
static gchar* opt_semantics = NULL;
static gchar* opt_template = NULL;

static JSemantics* j_benchmark_semantics = NULL;

static GTimer* j_benchmark_timer = NULL;

JSemantics*
j_benchmark_get_semantics(void)
{
	return j_semantics_ref(j_benchmark_semantics);
}

void
j_benchmark_timer_start(void)
{
	g_timer_start(j_benchmark_timer);
}

gdouble
j_benchmark_timer_elapsed(void)
{
	return g_timer_elapsed(j_benchmark_timer, NULL);
}

void
j_benchmark_run(gchar const* name, BenchmarkFunc benchmark_func)
{
	BenchmarkResult result;
	GTimer* func_timer;
	g_autofree gchar* left = NULL;
	gdouble elapsed;

	g_return_if_fail(name != NULL);
	g_return_if_fail(benchmark_func != NULL);

	if (opt_path != NULL)
	{
		g_autofree gchar* path_suite = NULL;

		path_suite = g_strconcat(opt_path, "/", NULL);

		if (g_strcmp0(name, opt_path) != 0 && !g_str_has_prefix(name, path_suite))
		{
			return;
		}
	}

	func_timer = g_timer_new();
	result.elapsed_time = 0.0;
	result.operations = 0;
	result.bytes = 0;

	if (!opt_machine_readable)
	{
		left = g_strconcat(name, ":", NULL);
		g_print("%-50s ", left);
	}
	else
	{
		g_print("%s", name);
	}

	g_timer_start(func_timer);
	(*benchmark_func)(&result);
	elapsed = g_timer_elapsed(func_timer, NULL);

	if (!opt_machine_readable)
	{
		g_print("%.3f seconds", result.elapsed_time);

		if (result.operations != 0)
		{
			g_print(" (%.0f/s)", (gdouble)result.operations / result.elapsed_time);
		}

		if (result.bytes != 0)
		{
			g_autofree gchar* size = NULL;

			size = g_format_size((gdouble)result.bytes / result.elapsed_time);
			g_print(" (%s/s)", size);
		}

		g_print(" [%.3f seconds]\n", elapsed);
	}
	else
	{
		g_print("%s%f", opt_machine_separator, result.elapsed_time);

		if (result.operations != 0)
		{
			g_print("%s%f", opt_machine_separator, (gdouble)result.operations / result.elapsed_time);
		}
		else
		{
			g_print("%s-", opt_machine_separator);
		}

		if (result.bytes != 0)
		{
			g_print("%s%f", opt_machine_separator, (gdouble)result.bytes / result.elapsed_time);
		}
		else
		{
			g_print("%s-", opt_machine_separator);
		}

		g_print("%s%f\n", opt_machine_separator, elapsed);
	}

	g_timer_destroy(func_timer);
}

int
main(int argc, char** argv)
{
	GError* error = NULL;
	GOptionContext* context;

	GOptionEntry entries[] = {
		{ "machine-readable", 'm', 0, G_OPTION_ARG_NONE, &opt_machine_readable, "Produce machine-readable output", NULL },
		{ "machine-separator", 0, 0, G_OPTION_ARG_STRING, &opt_machine_separator, "Separator for machine-readable output", "\\t" },
		{ "path", 'p', 0, G_OPTION_ARG_STRING, &opt_path, "Benchmark path to use", NULL },
		{ "semantics", 's', 0, G_OPTION_ARG_STRING, &opt_semantics, "Semantics to use", NULL },
		{ "template", 't', 0, G_OPTION_ARG_STRING, &opt_template, "Semantics template to use", NULL },
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

	if (opt_machine_separator == NULL)
	{
		opt_machine_separator = g_strdup("\t");
	}

	j_benchmark_semantics = j_semantics_new_from_string(opt_template, opt_semantics);
	j_benchmark_timer = g_timer_new();

	if (opt_machine_readable)
	{
		g_print("name%selapsed%soperations%sbytes%stotal_elapsed\n", opt_machine_separator, opt_machine_separator, opt_machine_separator, opt_machine_separator);
	}

	// Core
	benchmark_background_operation();
	benchmark_cache();
	benchmark_memory_chunk();
	benchmark_message();

	// KV client
	benchmark_kv();

	// Object client
	benchmark_distributed_object();
	benchmark_object();

	// Item client
	benchmark_collection();
	benchmark_item();

	// HDF5 client
	benchmark_hdf();

	g_timer_destroy(j_benchmark_timer);
	j_semantics_unref(j_benchmark_semantics);

	g_free(opt_machine_separator);
	g_free(opt_path);
	g_free(opt_semantics);
	g_free(opt_template);

	return 0;
}
