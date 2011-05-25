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

#ifndef H_ITEM_STATUS
#define H_ITEM_STATUS

struct JItemStatus;

typedef struct JItemStatus JItemStatus;

enum JItemStatusFlags
{
	J_ITEM_STATUS_NONE              =       0,
	J_ITEM_STATUS_SIZE              = (1 << 0),
	J_ITEM_STATUS_ACCESS_TIME       = (1 << 1),
	J_ITEM_STATUS_MODIFICATION_TIME = (1 << 2)
};

typedef enum JItemStatusFlags JItemStatusFlags;

#include <glib.h>

JItemStatus* j_item_status_new (JItemStatusFlags);
JItemStatus* j_item_status_ref (JItemStatus*);
void j_item_status_unref (JItemStatus*);

guint64 j_item_status_size (JItemStatus*);
void j_item_status_set_size (JItemStatus*, guint64);

gint64 j_item_status_access_time (JItemStatus*);
void j_item_status_set_access_time (JItemStatus*, gint64);

gint64 j_item_status_modification_time (JItemStatus*);
void j_item_status_set_modification_time (JItemStatus*, gint64);

#endif
