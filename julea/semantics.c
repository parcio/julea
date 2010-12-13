#include <glib.h>

#include "semantics.h"

struct JSemantics
{
};

JSemantics*
j_semantics_new (void)
{
	/*
: m_consistency(Consistency::Strict),
	m_persistency(Persistency::Strict),
	m_concurrency(Concurrency::Strict),
	m_security(Security::Strict)
	*/

	return g_new(JSemantics, 1);
}

void
j_semantics_set (JSemantics* semantics, gint key, gint value)
{
}

/*
using namespace std;
using namespace mongo;

namespace JULEA
{
	_Semantics::~_Semantics ()
	{
	}

	Consistency::Type const& _Semantics::GetConsistency () const
	{
		return m_consistency;
	}

	Persistency::Type const& _Semantics::GetPersistency () const
	{
		return m_persistency;
	}

	Concurrency::Type const& _Semantics::GetConcurrency () const
	{
		return m_concurrency;
	}

	Security::Type const& _Semantics::GetSecurity () const
	{
		return m_security;
	}
}
*/
