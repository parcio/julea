#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

#include "semantics.h"

using namespace std;
using namespace mongo;

namespace JULEA
{
	Semantics* Semantics::Ref ()
	{
		m_refCount++;

		return this;
	}

	bool Semantics::Unref ()
	{
		m_refCount--;

		if (m_refCount == 0)
		{
			delete this;

			return true;
		}

		return false;
	}

}
