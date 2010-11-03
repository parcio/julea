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

	Consistency::Type _Semantics::SetConsistency (Consistency::Type consistency)
	{
		m_consistency = consistency;

		return consistency;
	}

	Persistency::Type _Semantics::SetPersistency (Persistency::Type persistency)
	{
		m_persistency = persistency;

		return persistency;
	}

	Concurrency::Type _Semantics::SetConcurrency (Concurrency::Type concurrency)
	{
		m_concurrency = concurrency;

		return concurrency;
	}

	Security::Type _Semantics::SetSecurity (Security::Type security)
	{
		m_security = security;

		return security;
	}
}
