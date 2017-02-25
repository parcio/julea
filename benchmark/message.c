/*
 * Copyright (c) 2010-2017 Michael Kuhn
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

#include <jmessage-internal.h>

#include "benchmark.h"

static
void
benchmark_message_new (BenchmarkResult* result)
{
	guint const n = 500000;
	guint const m = 100;
	guint64 const dummy = 42;

	JMessage* message;
	gdouble elapsed;

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		message = j_message_new(J_MESSAGE_NONE, m * sizeof(guint64));

		for (guint j = 0; j < m; j++)
		{
			j_message_append_8(message, &dummy);
		}

		j_message_unref(message);
	}

	elapsed = j_benchmark_timer_elapsed();

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_message_add_operation (BenchmarkResult* result)
{
	guint const n = 500000;
	guint const m = 100;
	guint64 const dummy = 42;

	JMessage* message;
	gdouble elapsed;

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		message = j_message_new(J_MESSAGE_NONE, 0);

		for (guint j = 0; j < m; j++)
		{
			j_message_add_operation(message, sizeof(guint64));
			j_message_append_8(message, &dummy);
		}

		j_message_unref(message);
	}

	elapsed = j_benchmark_timer_elapsed();

	result->elapsed_time = elapsed;
	result->operations = n;
}

void
benchmark_message (void)
{
	j_benchmark_run("/message/new", benchmark_message_new);
	j_benchmark_run("/message/add_operation", benchmark_message_add_operation);
}
