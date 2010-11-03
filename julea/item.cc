#include <iostream>

#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "item.h"

#include "exception.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	BSONObj _Item::Serialize ()
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

	void _Item::Deserialize (BSONObj const& o)
	{
		m_id = o.getField("_id").OID();
		m_name = o.getField("Name").String();
	}

	void _Item::Associate (_Collection* collection)
	{
		if (m_collection != 0)
		{
			throw Exception("");
		}

		m_collection = collection->Ref();
	}

	_Item::_Item (string const& name)
		: m_name(name), m_collection(0), m_semantics(0)
	{
		m_id.clear();
	}

	_Item::_Item (_Collection* collection, BSONObj const& obj)
		: m_name(""), m_collection(collection->Ref()), m_semantics(0)
	{
		m_id.clear();

		Deserialize(obj);
	}

	_Item::~_Item ()
	{
		m_collection->Unref();

		if (m_semantics != 0)
		{
			m_semantics->Unref();
		}
	}

	string const& _Item::Name () const
	{
		return m_name;
	}

	void _Item::SetSemantics (Semantics semantics)
	{
		if (m_semantics != 0)
		{
			m_semantics->Unref();
		}

		m_semantics = semantics->Ref();
	}

	Semantics _Item::GetSemantics ()
	{
		return Semantics(m_semantics);
	}
}
