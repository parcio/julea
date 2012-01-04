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

#include <jbackground-operation-internal.h>

#include "test.h"

static
gpointer
on_background_operation_completed (gpointer data)
{
	return NULL;
}

static
void
test_background_operation_new_ref_unref (void)
{
	JBackgroundOperation* background_operation;

	background_operation = j_background_operation_new(on_background_operation_completed, NULL);
	j_background_operation_ref(background_operation);
	j_background_operation_unref(background_operation);
	j_background_operation_unref(background_operation);
}

static
void
test_background_operation_wait (void)
{
	JBackgroundOperation* background_operation;

	background_operation = j_background_operation_new(on_background_operation_completed, NULL);

	j_background_operation_wait(background_operation);
	j_background_operation_unref(background_operation);
}

void
test_background_operation (void)
{
	g_test_add_func("/background_operation/new_ref_unref", test_background_operation_new_ref_unref);
	g_test_add_func("/background_operation/wait", test_background_operation_wait);
}
