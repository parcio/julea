#ifndef H_CONNECTION
#define H_CONNECTION

#include <boost/utility.hpp>

#include <mongo/client/connpool.h>

namespace JULEA
{
	class _Connection;
	class Connection;
}

#include "public.h"
#include "ref_counted.h"
#include "store.h"

namespace JULEA
{
	class _Connection : public RefCounted<_Connection>
	{
		friend class RefCounted<_Connection>;

		friend class Connection;

		friend class _Collection;
		friend class Collection;
		friend class _Store;

		public:
			void Connect ();

			std::list<string> GetServers ();

			_Connection* AddServer (string const&);

			Store Get (string const&);
		private:
			_Connection ();
			~_Connection ();

			mongo::ScopedDbConnection* GetMongoDB ();

			std::list<string> m_servers;
			std::string m_servers_string;
	};

	class Connection : public Public<_Connection>
	{
		public:
			Connection ()
			{
				m_p = new _Connection();
			}
	};
}

#endif
