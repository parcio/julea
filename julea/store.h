#ifndef H_STORE
#define H_STORE

#include <boost/utility.hpp>

#include <mongo/db/jsobj.h>

namespace JULEA
{
	class Store;
}

namespace JULEA
{
	class Store
	{
		public:
			static void Initialize (string const&);

			static string const& Host ();
		private:
			static string m_host;
	};
}

#endif
