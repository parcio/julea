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

#ifndef H_SEMANTICS
#define H_SEMANTICS

#include <glib.h>

enum JSemanticsType
{
	J_SEMANTICS_CONSISTENCY,
	J_SEMANTICS_PERSISTENCY,
	J_SEMANTICS_CONCURRENCY,
	J_SEMANTICS_REDUNDANCY,
	J_SEMANTICS_SECURITY
};

typedef enum JSemanticsType JSemanticsType;

enum
{
	J_SEMANTICS_CONSISTENCY_STRICT
};

enum
{
	J_SEMANTICS_PERSISTENCY_STRICT,
	J_SEMANTICS_PERSISTENCY_LAX
};

enum
{
	J_SEMANTICS_CONCURRENCY_STRICT
};

enum
{
	J_SEMANTICS_REDUNDANCY_NONE
};

enum
{
	J_SEMANTICS_SECURITY_STRICT
};

struct JSemantics;

typedef struct JSemantics JSemantics;

JSemantics* j_semantics_new (void);
JSemantics* j_semantics_ref (JSemantics*);
void j_semantics_unref (JSemantics*);

void j_semantics_set (JSemantics*, JSemanticsType, gint);
gint j_semantics_get (JSemantics*, JSemanticsType);

#endif
