#ifndef H_STORE
#define H_STORE

#include <boost/utility.hpp>

#include <mongo/db/jsobj.h>

namespace JULEA
{
	class Store;
}

#include "collection.h"

namespace JULEA
{
	class Store
	{
		friend class Collection;

		public:
			Store (string const&);

			string const& Host () const;

			Collection* Get (string const&);
		private:
			~Store ();

			Store* Ref ();
			bool Unref ();

			string m_host;

			unsigned int m_refCount;
	};
}

#endif
