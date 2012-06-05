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

#include <jbackground-operation-internal.h>

#include "benchmark.h"

gint benchmark_background_operation_counter;

static
gpointer
on_background_operation_completed (gpointer data)
{
	g_atomic_int_inc(&benchmark_background_operation_counter);

	return NULL;
}

static
gchar*
benchmark_background_operation_new_ref_unref (void)
{
	guint const n = 100000;

	JBackgroundOperation* background_operation;
	gdouble elapsed;

	g_atomic_int_set(&benchmark_background_operation_counter, 0);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		background_operation = j_background_operation_new(on_background_operation_completed, NULL);
		j_background_operation_unref(background_operation);
	}

	/* FIXME overhead? */
	while (g_atomic_int_get(&benchmark_background_operation_counter) != (gint)n);

	elapsed = j_benchmark_timer_elapsed();

	return g_strdup_printf("%f seconds (%f/s)", elapsed, (gdouble)n / elapsed);
}

void
benchmark_background_operation (void)
{
	j_benchmark_run("/background_operation", benchmark_background_operation_new_ref_unref);
}
