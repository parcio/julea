#ifndef H_ITEM
#define H_ITEM

#include <boost/utility.hpp>

#include <mongo/db/jsobj.h>

namespace JULEA
{
	class _Item;
	class Item;
}

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
			string const& Name () const;

			void SetSemantics (Semantics);
			Semantics GetSemantics ();
		private:
			_Item (string const&);
			_Item (_Collection*, mongo::BSONObj const&);
			~_Item ();

			mongo::BSONObj Serialize ();
			void Deserialize (mongo::BSONObj const&);

			void Associate (_Collection*);

			mongo::OID m_id;
			string m_name;

			_Collection* m_collection;
			_Semantics* m_semantics;
	};

	class Item : public Public<_Item>
	{
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

#endif
