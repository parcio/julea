/*
 * Copyright (c) 2010 Michael Kuhn
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

#ifndef H_CONNECTION
#define H_CONNECTION

struct JConnection;

typedef struct JConnection JConnection;

#include <glib.h>

#include "jstore.h"

JConnection* j_connection_new (void);
JConnection* j_connection_ref (JConnection*);
void j_connection_unref (JConnection*);

gboolean j_connection_connect (JConnection*, const gchar*);

JStore* j_connection_get (JConnection*, const gchar*);

/*
#include "public.h"
#include "ref_counted.h"
#include "store.h"

namespace JULEA
{
	class _Connection : public RefCounted<_Connection>
	{
		friend class RefCounted<_Connection>;

		friend class Connection;

		friend class _Collection;
		friend class Collection;
		friend class _Store;

		public:
			std::list<string> GetServers ();
		private:
			~_Connection ();

			mongo::ScopedDbConnection* GetMongoDB ();

			std::list<string> m_servers;
			std::string m_servers_string;
	};

	class Connection : public Public<_Connection>
	{
		public:
			Connection ()
			{
				m_p = new _Connection();
			}
	};
}
*/

#endif
