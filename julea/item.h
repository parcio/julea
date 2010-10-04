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

namespace JULEA
{
	class _Item : public boost::noncopyable
	{
		friend class _Collection;

		public:
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

	class Item
	{
		public:
			Item (string const&);
			~Item ();

			Item& operator-> ();

		private:
			_Item* m_p;
	};
}

#endif
