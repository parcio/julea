#ifndef H_CONNECTION
#define H_CONNECTION

struct JConnection;

typedef struct JConnection JConnection;

#include <glib.h>

#include "store.h"

JConnection* j_connection_new (void);
void j_connection_connect (JConnection*);
void j_connection_add_server (JConnection*, const gchar*);
JStore* j_connection_get (JConnection*, const gchar*);

/*
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
			std::list<string> GetServers ();
		private:
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
*/

#endif
