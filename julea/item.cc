#include <iostream>

#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "item.h"

#include "store.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	BSONObj Item::Serialize ()
	{
		BSONObj o;

		if (!m_id.isSet())
		{
			m_id.init();
		}

		o = BSONObjBuilder()
			.append("_id", m_id)
			.append("Collection", m_collection->ID())
			.append("Name", m_name)
			.obj();

		return o;
	}

	void Item::Deserialize (BSONObj const& o)
	{
		m_id = o.getField("_id").OID();
		//m_collectionID = o.getField("Collection").OID();
		//m_name = o.getField("Name").String();
	}

	Item::Item (Collection* collection, string const& name)
		: m_name(name), m_collection(collection->Ref())
	{
		m_id.clear();

		ScopedDbConnection c(m_collection->m_store->Host());

		auto_ptr<DBClientCursor> cur = c->query("JULEA.Items", BSONObjBuilder().append("Collection", m_collection->ID()).append("Name", m_name).obj(), 1);

		if (cur->more())
		{
			Deserialize(cur->next());
		}

		c.done();
	}

	Item::~Item ()
	{
		m_collection->Unref();
	}

	string const& Item::Name () const
	{
		return m_name;
	}
}
