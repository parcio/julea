#ifndef H_STORE
#define H_STORE

#include <boost/utility.hpp>

#include <mongo/db/jsobj.h>

namespace JULEA
{
	class Store;
}

#include "collection.h"
#include "ref_counted.h"

namespace JULEA
{
	class Store : public RefCounted<Store>
	{
		friend class Collection;

		public:
			Store (string const&);

			string const& Host () const;

			std::map<string, Collection*> Get (std::list<string>);
			void Create (std::list<Collection*>);
		private:
			~Store ();

			string m_host;
			std::map<string, Collection*> m_collections;
	};
}

#endif
