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

namespace JULEA
{
	class _Item : public RefCounted<_Item>
	{
		friend class _Collection;

		public:
			_Item (string const&);
			_Item (_Collection*, string const&);

			string const& Name () const;
		private:
			~_Item ();

			mongo::BSONObj Serialize ();
			void Deserialize (mongo::BSONObj const&);

			mongo::OID m_id;
			string m_name;

			_Collection* m_collection;
	};

	class Item : public Public<_Item>
	{
		Item (string const& name)
		{
			m_p = new _Item(name);
		}
	};
}

#endif
