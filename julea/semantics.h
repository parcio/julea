#ifndef H_SEMANTICS
#define H_SEMANTICS

#include <boost/utility.hpp>

#include <mongo/db/jsobj.h>

namespace JULEA
{
	class Semantics;
}

#include "collection.h"

namespace JULEA
{
	class Semantics
	{
		public:
			Semantics ();
		private:
			~Semantics ();

			Semantics* Ref ();
			bool Unref ();

			unsigned int m_refCount;
	};
}

#endif
