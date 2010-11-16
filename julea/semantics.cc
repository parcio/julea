#include "semantics.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	_Semantics::_Semantics ()
	{
		m_consistency = Consistency::Strict;
		m_persistency = Persistency::Strict;
		m_concurrency = Concurrency::Strict;
		m_security = Security::Strict;
	}

	_Semantics::~_Semantics ()
	{
	}

	Consistency::Type const& _Semantics::GetConsistency () const
	{
		return m_consistency;
	}

	_Semantics* _Semantics::SetConsistency (Consistency::Type consistency)
	{
		m_consistency = consistency;

		return this;
	}

	Persistency::Type const& _Semantics::GetPersistency () const
	{
		return m_persistency;
	}

	_Semantics* _Semantics::SetPersistency (Persistency::Type persistency)
	{
		m_persistency = persistency;

		return this;
	}

	Concurrency::Type const& _Semantics::GetConcurrency () const
	{
		return m_concurrency;
	}

	_Semantics* _Semantics::SetConcurrency (Concurrency::Type concurrency)
	{
		m_concurrency = concurrency;

		return this;
	}

	Security::Type const& _Semantics::GetSecurity () const
	{
		return m_security;
	}

	_Semantics* _Semantics::SetSecurity (Security::Type security)
	{
		m_security = security;

		return this;
	}
}
