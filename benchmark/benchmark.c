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
static gchar* opt_namespace = NULL;

static JSemantics* j_benchmark_semantics = NULL;

static GList* j_benchmarks = NULL;
static gsize j_benchmark_name_max = 0;

JSemantics*
j_benchmark_get_semantics(void)
{
	return j_semantics_ref(j_benchmark_semantics);
}

void
j_benchmark_timer_start(BenchmarkRun* run)
{
	g_return_if_fail(run != NULL);

	if (!run->timer_started)
	{
		g_timer_start(run->timer);
		run->timer_started = TRUE;
	}
	else
	{
		g_timer_continue(run->timer);
	}
}

void
j_benchmark_timer_stop(BenchmarkRun* run)
{
	g_return_if_fail(run != NULL);

	g_timer_stop(run->timer);
}

gboolean
j_benchmark_iterate(BenchmarkRun* run)
{
	g_return_val_if_fail(run != NULL, FALSE);

	if (run->iterations == 0 || g_timer_elapsed(run->timer, NULL) < opt_duration)
	{
		run->iterations++;

		return TRUE;
	}

	return FALSE;
}

void
j_benchmark_add(gchar const* name, BenchmarkFunc benchmark_func)
{
	BenchmarkRun* run;

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

	j_benchmark_name_max = MAX(j_benchmark_name_max, strlen(name));

	run = g_new(BenchmarkRun, 1);
	run->name = g_strdup(name);
	run->func = benchmark_func;
	run->timer = g_timer_new();
	run->timer_started = FALSE;
	run->iterations = 0;
	run->operations = 0;
	run->bytes = 0;
	run->namespace = opt_namespace ? opt_namespace : "benchmark";

	j_benchmarks = g_list_prepend(j_benchmarks, run);
}

static void
j_benchmark_run_free(gpointer data)
{
	BenchmarkRun* run = data;

	g_return_if_fail(data != NULL);

	g_timer_destroy(run->timer);
	g_free(run->name);
	g_free(run);
}

static void
j_benchmark_run_one(BenchmarkRun* run)
{
	g_autoptr(GTimer) func_timer = NULL;
	gdouble elapsed_time;
	gdouble elapsed_total;

	g_return_if_fail(run != NULL);

	if (j_benchmark_name_max == 0)
	{
		return;
	}

	if (opt_list)
	{
		g_print("%s\n", run->name);
		return;
	}

	func_timer = g_timer_new();

	if (!opt_machine_readable)
	{
		g_autofree gchar* left = NULL;
		gsize pad;

		left = g_strconcat(run->name, ":", NULL);
		pad = j_benchmark_name_max + 2 - strlen(left);

		g_print("%s", left);

		for (guint i = 0; i < pad; i++)
		{
			g_print(" ");
		}
	}
	else
	{
		g_print("%s", run->name);
	}

	g_timer_start(func_timer);
	(*run->func)(run);
	elapsed_total = g_timer_elapsed(func_timer, NULL);

	elapsed_time = g_timer_elapsed(run->timer, NULL);

	if (run->iterations > 1)
	{
		run->operations *= run->iterations;
		run->bytes *= run->iterations;
	}

	if (!opt_machine_readable)
	{
		g_print("%.3f seconds", elapsed_time);

		if (run->operations != 0)
		{
			g_print(" (%.0f/s)", (gdouble)run->operations / elapsed_time);
		}

		if (run->bytes != 0)
		{
			g_autofree gchar* size = NULL;

			size = g_format_size((gdouble)run->bytes / elapsed_time);
			g_print(" (%s/s)", size);
		}

		g_print(" [%.3f seconds]\n", elapsed_total);
	}
	else
	{
		g_print("%s%f", opt_machine_separator, elapsed_time);

		if (run->operations != 0)
		{
			g_print("%s%f", opt_machine_separator, (gdouble)run->operations / elapsed_time);
		}
		else
		{
			g_print("%s-", opt_machine_separator);
		}

		if (run->bytes != 0)
		{
			g_print("%s%f", opt_machine_separator, (gdouble)run->bytes / elapsed_time);
		}
		else
		{
			g_print("%s-", opt_machine_separator);
		}

		g_print("%s%f\n", opt_machine_separator, elapsed_total);
	}

	if (run->iterations > 1000 * (guint)opt_duration)
	{
		g_warning("Benchmark %s performed %d iteration(s) in a duration of %d second(s), consider adjusting iteration workload.", run->name, run->iterations, opt_duration);
	}
}

static void
j_benchmark_run_all(void)
{
	GList* benchmark;

	if (j_benchmarks == NULL || j_benchmark_name_max == 0)
	{
		return;
	}

	if (!opt_machine_readable)
	{
		gchar const* left;
		gchar const* right;
		gsize pad;

		left = "Name";
		right = "Duration (Operations/s) (Throughput/s) [Total Duration]";
		pad = j_benchmark_name_max + 2 - strlen(left);

		g_print("Name");

		for (guint i = 0; i < pad; i++)
		{
			g_print(" ");
		}

		g_print("%s\n", right);

		for (guint i = 0; i < j_benchmark_name_max + 2 + strlen(right); i++)
		{
			g_print("-");
		}

		g_print("\n");
	}
	else
	{
		g_print("name%selapsed%soperations%sbytes%stotal_elapsed\n", opt_machine_separator, opt_machine_separator, opt_machine_separator, opt_machine_separator);
	}

	j_benchmarks = g_list_reverse(j_benchmarks);

	for (benchmark = j_benchmarks; benchmark != NULL; benchmark = benchmark->next)
	{
		j_benchmark_run_one(benchmark->data);
	}

	g_list_free_full(j_benchmarks, j_benchmark_run_free);
	j_benchmarks = NULL;
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
		{ "namespace", 'n', 0, G_OPTION_ARG_STRING, &opt_namespace, "Namespace for benchmark to use", NULL },
		{ NULL, 0, 0, 0, NULL, NULL, NULL }
	};

	// Explicitly enable UTF-8 since functions such as g_format_size might return UTF-8 characters.
	setlocale(LC_ALL, "C.UTF-8");

	context = g_option_context_new(NULL);
	g_option_context_add_main_entries(context, entries, NULL);

	if (!g_option_context_parse(context, &argc, &argv, &error))
	{
		g_option_context_free(context);

		if (error != NULL)
		{
			g_printerr("Error: %s\n", error->message);
			g_error_free(error);
		}

		return 1;
	}

	g_option_context_free(context);

	if (opt_duration <= 0)
	{
		g_printerr("Error: Duration has to be greater than 0\n");

		return 1;
	}

	if (opt_machine_separator == NULL)
	{
		opt_machine_separator = g_strdup("\t");
	}

	j_benchmark_semantics = j_semantics_new_from_string(opt_template, opt_semantics);

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
	benchmark_db_entry();
	benchmark_db_iterator();
	benchmark_db_schema();

	// Item client
	benchmark_collection();
	benchmark_item();

	// HDF5 client
	benchmark_hdf();
	benchmark_hdf_dai();

	j_benchmark_run_all();

	j_semantics_unref(j_benchmark_semantics);

	g_free(opt_machine_separator);
	g_free(opt_path);
	g_free(opt_semantics);
	g_free(opt_template);
	g_free(opt_namespace);

	return 0;
}
