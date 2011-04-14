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

#ifndef H_TRACE
#define H_TRACE

enum JTraceFileOperation
{
	J_TRACE_FILE_OPEN,
	J_TRACE_FILE_CLOSE,
	J_TRACE_FILE_READ,
	J_TRACE_FILE_WRITE,
	J_TRACE_FILE_SEEK
};

typedef enum JTraceFileOperation JTraceFileOperation;

struct JTrace;

typedef struct JTrace JTrace;

struct JTraceFile;

typedef struct JTraceFile JTraceFile;

#include <glib.h>

void j_trace_init (gchar const*);
void j_trace_deinit (void);

void j_trace_enter (JTrace*, gchar const*);
void j_trace_leave (JTrace*, gchar const*);

void j_trace_file_begin (JTrace*, gchar const*, JTraceFileOperation);
void j_trace_file_end (JTrace*, gchar const*, JTraceFileOperation, guint64);

JTrace* j_trace_thread_enter (GThread*, gchar const*);
void j_trace_thread_leave (JTrace*);

#endif
