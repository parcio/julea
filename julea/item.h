#ifndef H_ITEM
#define H_ITEM

#include <boost/utility.hpp>

#include <mongo/db/jsobj.h>

namespace JULEA
{
	class Item;
}

#include "collection.h"

namespace JULEA
{
	class Item : public boost::noncopyable
	{
		friend class Collection;

		public:
			Item (Collection const*, string const&);

			string const& Name () const;
		private:
			~Item ();

			mongo::BSONObj Serialize ();
			void Deserialize (mongo::BSONObj const&);

			mongo::OID m_id;
			string m_name;

			Collection const* m_collection;
	};
}

#endif
