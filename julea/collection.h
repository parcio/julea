#ifndef H_COLLECTION
#define H_COLLECTION

#include <list>

#include <boost/utility.hpp>

#include <mongo/db/jsobj.h>

namespace JULEA
{
	class _Collection;
	class Collection;
}

#include "item.h"
#include "public.h"
#include "ref_counted.h"
#include "store.h"

namespace JULEA
{
	class _Collection : public RefCounted<_Collection>
	{
		friend class Collection;

		friend class _Item;
		friend class _Store;

		public:
			/*
			class Iterator
			{
				public:
					Iterator (_Collection const& directory) : connection(FileSystem::Host()), directory(directory) { cursor =  connection->query("JULEA.DirectoryEntries", mongo::BSONObjBuilder().append("_Collection", directory.m_id).obj()); };
					~Iterator () { connection.done(); };

					bool More () { return cursor->more(); };
					_Item Next () { mongo::BSONObj f; string name; f = cursor->next(); name = f.getField("Name").String(); return _Item(name); };
				private:
					mongo::ScopedDbConnection connection;
					_Collection const& directory;
					std::auto_ptr<mongo::DBClientCursor> cursor;
			};
			*/

		public:
			string const& Name () const;

			_Item* Get (string const&);
		private:
			_Collection (string const&);
			_Collection (_Store*, string const&);
			_Collection (_Store*, mongo::BSONObj const&);
			~_Collection ();

			mongo::BSONObj Serialize ();
			void Deserialize (mongo::BSONObj const&);

			mongo::OID const& ID () const;

			mongo::OID m_id;
			string m_name;
			std::list<_Item*> newItems;

			_Store* m_store;
	};

	class Collection : public Public<_Collection>
	{
		public:
			Collection (string const& name)
			{
				m_p = new _Collection(name);
			}
	};
}

#endif
