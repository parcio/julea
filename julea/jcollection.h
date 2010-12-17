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
 */

#ifndef H_COLLECTION
#define H_COLLECTION

struct JCollection;

typedef struct JCollection JCollection;

#include <glib.h>

#include "jsemantics.h"

JCollection* j_collection_new (const gchar*);
JCollection* j_collection_ref (JCollection*);
void j_collection_unref (JCollection*);

const gchar* j_collection_name (JCollection*);

void j_collection_create (JCollection*, GQueue*);
GQueue* j_collection_get (JCollection*, GQueue*);

JSemantics* j_collection_semantics (JCollection*);
void j_collection_set_semantics (JCollection*, JSemantics*);

/*
#include "credentials.h"
#include "item.h"
#include "public.h"
#include "ref_counted.h"
#include "semantics.h"
#include "store.h"

namespace JULEA
{
	class _Collection : public RefCounted<_Collection>
	{
		friend class RefCounted<_Collection>;

		friend class Collection;

		friend class _Item;
		friend class _Store;

		public:
			std::list<Item> Get (std::list<string>);
		private:
			_Collection (_Store*, mongo::BSONObj const&);

			void IsInitialized (bool) const;

			void Deserialize (mongo::BSONObj const&);

			mongo::OID const& ID () const;

			bool m_initialized;

			Credentials m_owner;
	};

	class Collection : public Public<_Collection>
	{
		friend class _Store;

		public:
			Collection (string const& name)
			{
				m_p = new _Collection(name);
			}

		private:
			Collection (_Store* store, mongo::BSONObj const& obj)
			{
				m_p = new _Collection(store, obj);
			}
	};
}
*/

#endif
