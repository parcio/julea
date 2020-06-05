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

#include <julea.h>

#include <jmessage.h>

#include "benchmark.h"

static void
_benchmark_message_new(BenchmarkRun* run, gboolean append)
{
	guint const n = 100000;
	guint const m = 100;
	guint64 const dummy = 42;
	gsize const size = m * sizeof(guint64);

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			g_autoptr(JMessage) message = NULL;

			message = j_message_new(J_MESSAGE_NONE, (append) ? size : 0);

			if (append)
			{
				for (guint j = 0; j < m; j++)
				{
					j_message_append_8(message, &dummy);
				}
			}
		}
	}

	j_benchmark_timer_stop(run);

	run->operations = n;
}

static void
benchmark_message_new(BenchmarkRun* run)
{
	_benchmark_message_new(run, FALSE);
}

static void
benchmark_message_new_append(BenchmarkRun* run)
{
	_benchmark_message_new(run, TRUE);
}

static void
_benchmark_message_add_operation(BenchmarkRun* run, gboolean large)
{
	guint const n = (large) ? 100 : 10000;
	guint const m = (large) ? 10000 : 100;
	guint64 const dummy = 42;

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			g_autoptr(JMessage) message = NULL;

			message = j_message_new(J_MESSAGE_NONE, 0);

			for (guint j = 0; j < m; j++)
			{
				j_message_add_operation(message, sizeof(guint64));
				j_message_append_8(message, &dummy);
			}
		}
	}

	j_benchmark_timer_stop(run);

	run->operations = n;
	run->bytes = n * m * sizeof(guint64);
}

static void
benchmark_message_add_operation_small(BenchmarkRun* run)
{
	_benchmark_message_add_operation(run, FALSE);
}

static void
benchmark_message_add_operation_large(BenchmarkRun* run)
{
	_benchmark_message_add_operation(run, TRUE);
}

void
benchmark_message(void)
{
	j_benchmark_add("/message/new", benchmark_message_new);
	j_benchmark_add("/message/new-append", benchmark_message_new_append);
	j_benchmark_add("/message/add-operation-small", benchmark_message_add_operation_small);
	j_benchmark_add("/message/add-operation-large", benchmark_message_add_operation_large);
}
