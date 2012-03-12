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

/**
 * \file
 **/

#include <glib.h>

#include <jthread-internal.h>

#include <jstatistics-internal.h>
#include <jtrace-internal.h>

/**
 * \defgroup JThread Thread
 *
 * @{
 **/

/**
 * A thread.
 **/
struct JThread
{
	JStatistics* statistics;
	JTrace* trace;
};

/**
 * Creates a new thread.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * JThread* c;
 *
 * c = j_thread_new();
 * \endcode
 *
 * \return A new thread. Should be freed with j_thread_free().
 **/
JThread*
j_thread_new (GThread* thread_id, gchar const* function_name)
{
	JThread* thread;

	g_return_val_if_fail(function_name != NULL, NULL);

	thread = g_slice_new(JThread);
	thread->trace = j_trace_thread_enter(thread_id, function_name);
	thread->statistics = j_statistics_new(thread->trace);

	return thread;
}

/**
 * Frees the memory allocated for the thread.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param thread A thread.
 **/
void
j_thread_free (JThread* thread)
{
	g_return_if_fail(thread != NULL);

	j_trace_thread_leave(thread->trace);
	j_statistics_free(thread->statistics);

	g_slice_free(JThread, thread);
}

JStatistics*
j_thread_get_statistics (JThread* thread)
{
	g_return_val_if_fail(thread != NULL, NULL);

	return thread->statistics;
}

JTrace*
j_thread_get_trace (JThread* thread)
{
	g_return_val_if_fail(thread != NULL, NULL);

	return thread->trace;
}

/**
 * @}
 **/
