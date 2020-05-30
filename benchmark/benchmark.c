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

#include <locale.h>
#include <string.h>

#include <julea.h>

#include "benchmark.h"

static gint opt_duration = 1;
static gboolean opt_list = FALSE;
static gchar* opt_machine_separator = NULL;
static gboolean opt_machine_readable = FALSE;
static gchar* opt_path = NULL;
static gchar* opt_semantics = NULL;
static gchar* opt_template = NULL;

static JSemantics* j_benchmark_semantics = NULL;

static GTimer* j_benchmark_timer = NULL;
static gboolean j_benchmark_timer_started = FALSE;

static guint j_benchmark_iterations = 0;

JSemantics*
j_benchmark_get_semantics(void)
{
	return j_semantics_ref(j_benchmark_semantics);
}

void
j_benchmark_timer_start(void)
{
	if (!j_benchmark_timer_started)
	{
		g_timer_start(j_benchmark_timer);
		j_benchmark_timer_started = TRUE;
	}
	else
	{
		g_timer_continue(j_benchmark_timer);
	}
}

void
j_benchmark_timer_stop(void)
{
	g_timer_stop(j_benchmark_timer);
}

gboolean
j_benchmark_iterate(void)
{
	if (j_benchmark_iterations == 0 || g_timer_elapsed(j_benchmark_timer, NULL) < opt_duration)
	{
		j_benchmark_iterations++;

		return TRUE;
	}

	return FALSE;
}

void
j_benchmark_run(gchar const* name, BenchmarkFunc benchmark_func)
{
	BenchmarkResult result;
	g_autoptr(GTimer) func_timer = NULL;
	g_autofree gchar* left = NULL;
	gdouble elapsed_time;
	gdouble elapsed_total;

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

	if (opt_list)
	{
		g_print("%s\n", name);
		return;
	}

	func_timer = g_timer_new();
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

	j_benchmark_timer_started = FALSE;
	j_benchmark_iterations = 0;

	g_timer_start(func_timer);
	(*benchmark_func)(&result);
	elapsed_total = g_timer_elapsed(func_timer, NULL);

	elapsed_time = g_timer_elapsed(j_benchmark_timer, NULL);

	if (j_benchmark_iterations > 1)
	{
		result.operations *= j_benchmark_iterations;
		result.bytes *= j_benchmark_iterations;
	}

	if (!opt_machine_readable)
	{
		g_print("%.3f seconds", elapsed_time);

		if (result.operations != 0)
		{
			g_print(" (%.0f/s)", (gdouble)result.operations / elapsed_time);
		}

		if (result.bytes != 0)
		{
			g_autofree gchar* size = NULL;

			size = g_format_size((gdouble)result.bytes / elapsed_time);
			g_print(" (%s/s)", size);
		}

		g_print(" [%.3f seconds]\n", elapsed_total);
	}
	else
	{
		g_print("%s%f", opt_machine_separator, elapsed_time);

		if (result.operations != 0)
		{
			g_print("%s%f", opt_machine_separator, (gdouble)result.operations / elapsed_time);
		}
		else
		{
			g_print("%s-", opt_machine_separator);
		}

		if (result.bytes != 0)
		{
			g_print("%s%f", opt_machine_separator, (gdouble)result.bytes / elapsed_time);
		}
		else
		{
			g_print("%s-", opt_machine_separator);
		}

		g_print("%s%f\n", opt_machine_separator, elapsed_total);
	}

	if (j_benchmark_iterations > 1000 * (guint)opt_duration)
	{
		g_warning("Benchmark %s performed %d iterations, consider adjusting iteration workload.", name, j_benchmark_iterations);
	}
}

int
main(int argc, char** argv)
{
	GError* error = NULL;
	GOptionContext* context;

	GOptionEntry entries[] = {
		{ "duration", 'd', 0, G_OPTION_ARG_INT, &opt_duration, "Approximate duration in seconds per benchmark", "1" },
		{ "list", 'l', 0, G_OPTION_ARG_NONE, &opt_list, "List available benchmarks", NULL },
		{ "machine-readable", 'm', 0, G_OPTION_ARG_NONE, &opt_machine_readable, "Produce machine-readable output", NULL },
		{ "machine-separator", 0, 0, G_OPTION_ARG_STRING, &opt_machine_separator, "Separator for machine-readable output", "\\t" },
		{ "path", 'p', 0, G_OPTION_ARG_STRING, &opt_path, "Benchmark path to use", NULL },
		{ "semantics", 's', 0, G_OPTION_ARG_STRING, &opt_semantics, "Semantics to use", NULL },
		{ "template", 't', 0, G_OPTION_ARG_STRING, &opt_template, "Semantics template to use", NULL },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	// Explicitly enable UTF-8 since functions such as g_format_size might return UTF-8 characters.
	setlocale(LC_ALL, "C.UTF-8");

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

	// DB client
	benchmark_db_schema();

	// Item client
	benchmark_collection();
	benchmark_item();

	// HDF5 client
	benchmark_hdf();
	benchmark_hdf_dai();

	g_timer_destroy(j_benchmark_timer);
	j_semantics_unref(j_benchmark_semantics);

	g_free(opt_machine_separator);
	g_free(opt_path);
	g_free(opt_semantics);
	g_free(opt_template);

	return 0;
}
