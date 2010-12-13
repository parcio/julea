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
