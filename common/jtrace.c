/*
 * Copyright (c) 2010-2011 Michael Kuhn
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

#include <string.h>

#ifdef HAVE_OTF
#include <otf.h>
#endif

#include "jtrace.h"

/**
 * \defgroup JTrace Trace
 *
 * @{
 **/

#ifdef HAVE_OTF
static OTF_FileManager* otf_manager = NULL;
static OTF_Writer* otf_writer = NULL;

static guint32 otf_process_id = 1;
static guint32 otf_function_id = 1;
#endif

void
j_trace_init (const gchar* name)
{
#ifdef HAVE_OTF
	otf_manager = OTF_FileManager_open(1);
	g_assert(otf_manager != NULL);

	otf_writer = OTF_Writer_open(name, 1, otf_manager);
	g_assert(otf_writer != NULL);
#endif
}

void
j_trace_deinit (void)
{
#ifdef HAVE_OTF
	OTF_Writer_close(otf_writer);
	otf_writer = NULL;

	OTF_FileManager_close(otf_manager);
	otf_manager = NULL;
#endif
}

void
j_trace_define_process (const gchar* name)
{
#ifdef HAVE_OTF
	OTF_Writer_writeDefProcess(otf_writer, 1, otf_process_id, name, 0);
	otf_process_id++;
#endif
}

void
j_trace_define_function (const gchar* name)
{
#ifdef HAVE_OTF
	OTF_Writer_writeDefFunction(otf_writer, 1, otf_function_id, name, 0, 0);
	otf_function_id++;
#endif
}

void
j_trace_enter (void)
{
#ifdef HAVE_OTF
	GTimeVal timeval;
	guint64 timestamp;

	g_get_current_time(&timeval);
	timestamp = (timeval.tv_sec * G_USEC_PER_SEC) + timeval.tv_usec;

	OTF_Writer_writeEnter(otf_writer, timestamp, 1, 1, 0);
#endif
}

void
j_trace_leave (void)
{
#ifdef HAVE_OTF
	GTimeVal timeval;
	guint64 timestamp;

	g_get_current_time(&timeval);
	timestamp = (timeval.tv_sec * G_USEC_PER_SEC) + timeval.tv_usec;

	OTF_Writer_writeLeave(otf_writer, timestamp, 1, 1, 0);
#endif
}

/**
 * @}
 **/
