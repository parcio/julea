#ifndef H_STORE
#define H_STORE

#include <boost/utility.hpp>

#include <mongo/db/jsobj.h>

namespace JULEA
{
	class _Store;
	class Store;
}

#include "collection.h"
#include "public.h"
#include "ref_counted.h"

namespace JULEA
{
	class _Store : public RefCounted<_Store>
	{
		friend class RefCounted<_Store>;

		friend class Store;

		public:
			string const& Host () const;

			std::list<Collection> Get (std::list<string>);
			void Create (std::list<Collection>);
		private:
			_Store (string const&);
			~_Store ();

			string m_host;
	};

	class Store : public Public<_Store>
	{
		public:
			Store (string const& name)
			{
				m_p = new _Store(name);
			}
	};
}

#endif
