#ifndef H_COLLECTION
#define H_COLLECTION

/*
#include <list>

#include <boost/utility.hpp>

#include <mongo/db/jsobj.h>

namespace JULEA
{
	class _Collection;
	class Collection;
}

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
			string const& Name () const;

			std::list<Item> Get (std::list<string>);
			void Create (std::list<Item>);

			_Semantics const* GetSemantics ();
			void SetSemantics (Semantics const&);
		private:
			_Collection (string const&);
			_Collection (_Store*, mongo::BSONObj const&);
			~_Collection ();

			void IsInitialized (bool) const;

			mongo::BSONObj Serialize ();
			void Deserialize (mongo::BSONObj const&);

			std::string const& ItemsCollection ();

			mongo::OID const& ID () const;

			void Associate (_Store*);

			bool m_initialized;

			mongo::OID m_id;
			string m_name;

			Credentials m_owner;

			_Semantics* m_semantics;
			_Store* m_store;

			std::string m_itemsCollection;
	};

	class Collection : public Public<_Collection>
	{
		friend class _Store;

		public:
			class Iterator
			{
				public:
					Iterator (Collection const&);
					~Iterator ();

					bool More ();
					Item Next ();
				private:
					mongo::ScopedDbConnection* m_connection;
					_Collection* m_collection;
					std::auto_ptr<mongo::DBClientCursor> m_cursor;
			};

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
