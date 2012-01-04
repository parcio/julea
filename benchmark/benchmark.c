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

gchar* j_benchmark_filter = NULL;
GTimer* j_benchmark_timer = NULL;

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

	if (j_benchmark_filter != NULL && !g_str_has_prefix(name, j_benchmark_filter))
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
	j_init(&argc, &argv);

	if (argc > 1)
	{
		j_benchmark_filter = g_strdup(argv[1]);
	}

	j_benchmark_timer = g_timer_new();

	benchmark_background_operation();
	benchmark_collection();
	benchmark_item();

	g_free(j_benchmark_filter);
	g_timer_destroy(j_benchmark_timer);

	j_fini();

	return 0;
}
