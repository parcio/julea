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

#ifndef H_BACKEND
#define H_BACKEND

#include "jtrace.h"

struct JBackendFile
{
	gchar* path;
	gpointer user_data;
};

typedef struct JBackendFile JBackendFile;

void (*jd_backend_init) (gchar const*, JTrace*) = NULL;
void (*jd_backend_fini) (JTrace*) = NULL;

void (*jd_backend_open) (JBackendFile*, gchar const*, gchar const*, gchar const*, JTrace*) = NULL;
void (*jd_backend_close) (JBackendFile*, JTrace*) = NULL;

void (*jd_backend_sync) (JBackendFile*, JTrace*) = NULL;

guint64 (*jd_backend_read) (JBackendFile*, gpointer, guint64, guint64, JTrace*) = NULL;
guint64 (*jd_backend_write) (JBackendFile*, gconstpointer, guint64, guint64, JTrace*) = NULL;

#endif
