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

#ifndef H_CONNECTION_INTERNAL
#define H_CONNECTION_INTERNAL

#include <glib.h>

#include <julea-internal.h>

#include <jconfiguration-internal.h>
#include <jmessage.h>

#include <mongo.h>

#define J_CONNECTION_ERROR j_connection_error_quark()

enum JConnectionError
{
	J_CONNECTION_ERROR_FAILED
};

typedef enum JConnectionError JConnectionError;

struct JConnection;

typedef struct JConnection JConnection;

J_GNUC_INTERNAL GQuark j_connection_error_quark (void);

J_GNUC_INTERNAL JConnection* j_connection_new (JConfiguration*);
J_GNUC_INTERNAL JConnection* j_connection_ref (JConnection*);
J_GNUC_INTERNAL void j_connection_unref (JConnection*);

J_GNUC_INTERNAL gboolean j_connection_connect (JConnection*);
J_GNUC_INTERNAL gboolean j_connection_disconnect (JConnection*);

J_GNUC_INTERNAL mongo* j_connection_get_connection (JConnection*);

J_GNUC_INTERNAL gboolean j_connection_send (JConnection*, guint, JMessage*);
J_GNUC_INTERNAL gboolean j_connection_send_data (JConnection*, guint, gconstpointer, gsize);

J_GNUC_INTERNAL gboolean j_connection_receive (JConnection*, guint, JMessage*);
J_GNUC_INTERNAL gboolean j_connection_receive_data (JConnection*, guint, gpointer, gsize);

#endif
