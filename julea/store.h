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
#include "ref_counted.h"

namespace JULEA
{
	class _Store : public RefCounted<_Store>
	{
		friend class Store;

		friend class _Collection;

		public:
			_Store (string const&);

			string const& Host () const;

			std::map<string, _Collection*> Get (std::list<string>);
			void Create (std::list<_Collection*>);
		private:
			~_Store ();

			string m_host;
			std::map<string, _Collection*> m_collections;
	};

	class Store
	{
		public:
			Store (string const& name)
			{
				m_p = new _Store(name);
			}

			~Store ()
			{
				m_p->Unref();
			}

			_Store* operator-> ()
			{
				return m_p;
			}

		private:
			_Store* m_p;
	};
}

#endif
