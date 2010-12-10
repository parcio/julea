/*
#include <iostream>

#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "item.h"

#include "exception.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	_Item::_Item (string const& name)
		: m_initialized(false),
		  m_name(name),
		  m_collection(0),
		  m_semantics(0)
	{
		m_id.init();
	}

	_Item::_Item (_Collection* collection, BSONObj const& obj)
		: m_initialized(true),
		  m_name(""),
		  m_collection(collection->Ref()),
		  m_semantics(0)
	{
		m_id.init();

		Deserialize(obj);
	}

	_Item::~_Item ()
	{
		if (m_collection != 0)
		{
			m_collection->Unref();
		}

		if (m_semantics != 0)
		{
			m_semantics->Unref();
		}
	}

	void _Item::IsInitialized (bool check)
	{
		if (m_initialized != check)
		{
			if (check)
			{
				throw Exception(JULEA_FILELINE ": Item not initialized.");
			}
			else
			{
				throw Exception(JULEA_FILELINE ": Item already initialized.");
			}
		}
	}

	BSONObj _Item::Serialize ()
	{
		BSONObj o;

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
		IsInitialized(false);

		m_collection = collection->Ref();
		m_initialized = true;
	}

	string const& _Item::Name () const
	{
		return m_name;
	}

	_Semantics const* _Item::GetSemantics ()
	{
		if (m_semantics != 0)
		{
			return m_semantics;
		}

		return m_collection->GetSemantics();
	}

	void _Item::SetSemantics (Semantics const& semantics)
	{
		if (m_semantics != 0)
		{
			m_semantics->Unref();
		}

		m_semantics = semantics->Ref();
	}
}
*/
