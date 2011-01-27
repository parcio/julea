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

#ifndef H_ITEM
#define H_ITEM

struct JItem;

typedef struct JItem JItem;

#include <glib.h>

#include "jsemantics.h"

JItem* j_item_new (const gchar*);
void j_item_unref (JItem*);

const gchar* j_item_name (JItem*);

JSemantics* j_item_semantics (JItem*);
void j_item_set_semantics (JItem*, JSemantics*);

gboolean j_item_write (JItem*, gconstpointer, gsize, goffset);

/*
#include "collection.h"
#include "public.h"
#include "ref_counted.h"
#include "semantics.h"

namespace JULEA
{
	class _Item : public RefCounted<_Item>
	{
		friend class RefCounted<_Item>;

		friend class Item;

		friend class _Collection;

		public:
			_Semantics const* GetSemantics ();
		private:
			_Item (_Collection*, mongo::BSONObj const&);
			~_Item ();

			void IsInitialized (bool);

			mongo::BSONObj Serialize ();
			void Deserialize (mongo::BSONObj const&);

			void Associate (_Collection*);

			bool m_initialized;

			mongo::OID m_id;
			string m_name;

			_Collection* m_collection;
			_Semantics* m_semantics;
	};

	class Item : public Public<_Item>
	{
		friend class Collection;
		friend class _Collection;

		public:
			Item (string const& name)
			{
				m_p = new _Item(name);
			}

		private:
			Item (_Collection* collection, mongo::BSONObj const& obj)
			{
				m_p = new _Item(collection, obj);
			}
	};
}
*/

#endif
