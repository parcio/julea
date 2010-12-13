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

#ifndef H_STORE
#define H_STORE

struct JStore;

typedef struct JStore JStore;

#include <glib.h>

#include "collection.h"
#include "connection.h"

JStore* j_store_new (JConnection*, const gchar*);
void j_store_unref (JStore*);

void j_store_create (JStore*, GList*);

/*
#include "collection.h"
#include "connection.h"
#include "public.h"
#include "ref_counted.h"
#include "semantics.h"

namespace JULEA
{
	class _Store : public RefCounted<_Store>
	{
		friend class RefCounted<_Store>;

		friend class Store;

		friend class Collection;
		friend class _Collection;

		public:
			std::string const& Name ();

			std::list<Collection> Get (std::list<string>);

			_Semantics const* GetSemantics ();
			void SetSemantics (Semantics const&);
		private:
			_Store (string const&);
			~_Store ();

			void IsInitialized (bool) const;

			string const& CollectionsCollection ();

			_Connection* Connection ();

			bool m_initialized;

			std::string m_name;

			_Connection* m_connection;
			_Semantics* m_semantics;

			std::string m_collectionsCollection;
	};

	class Store : public Public<_Store>
	{
		friend class _Connection;

		public:
			Store (string const& name)
			{
				m_p = new _Store(name);
			}

		private:
			Store (_Connection* connection, string const& name)
			{
				m_p = new _Store(connection, name);
			}
	};
}
*/

#endif
