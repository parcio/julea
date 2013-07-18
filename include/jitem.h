/*
 * Copyright (c) 2010-2013 Michael Kuhn
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

#ifndef H_ITEM
#define H_ITEM

#include <glib.h>

enum JItemStatusFlags
{
	J_ITEM_STATUS_NONE              = 0,
	J_ITEM_STATUS_MODIFICATION_TIME = 1 << 0,
	J_ITEM_STATUS_SIZE              = 1 << 1,
	J_ITEM_STATUS_ALL               = (J_ITEM_STATUS_MODIFICATION_TIME | J_ITEM_STATUS_SIZE)
};

typedef enum JItemStatusFlags JItemStatusFlags;

struct JItem;

typedef struct JItem JItem;

#include <jbatch.h>
#include <jdistribution.h>
#include <jsemantics.h>

JItem* j_item_new (gchar const*, JDistribution*);
JItem* j_item_ref (JItem*);
void j_item_unref (JItem*);

gchar const* j_item_get_name (JItem*);

void j_item_read (JItem*, gpointer, guint64, guint64, guint64*, JBatch*);
void j_item_write (JItem*, gconstpointer, guint64, guint64, guint64*, JBatch*);

void j_item_get_status (JItem*, JItemStatusFlags, JBatch*);

guint64 j_item_get_size (JItem*);
gint64 j_item_get_modification_time (JItem*);

guint64 j_item_get_optimal_access_size (JItem*);

/*
		private:
			void IsInitialized (bool);

			bool m_initialized;
*/

#endif
