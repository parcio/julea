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

/**
 * \file
 **/

#ifndef H_BACKGROUND_OPERATION_INTERNAL
#define H_BACKGROUND_OPERATION_INTERNAL

#include <glib.h>

#include <julea-internal.h>

struct JBackgroundOperation;

typedef struct JBackgroundOperation JBackgroundOperation;

typedef gpointer (*JBackgroundOperationFunc) (gpointer);

J_GNUC_INTERNAL void j_background_operation_init (guint count);
J_GNUC_INTERNAL void j_background_operation_fini (void);

J_GNUC_INTERNAL guint j_background_operation_get_num_threads (void);

J_GNUC_INTERNAL JBackgroundOperation* j_background_operation_new (JBackgroundOperationFunc, gpointer);
J_GNUC_INTERNAL JBackgroundOperation* j_background_operation_ref (JBackgroundOperation*);
J_GNUC_INTERNAL void j_background_operation_unref (JBackgroundOperation*);

J_GNUC_INTERNAL gpointer j_background_operation_wait (JBackgroundOperation*);

#endif
