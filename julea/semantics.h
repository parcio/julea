#ifndef H_SEMANTICS
#define H_SEMANTICS

#include <boost/utility.hpp>

#include <mongo/db/jsobj.h>

namespace JULEA
{
	class Semantics;
}

#include "collection.h"
#include "ref_counted.h"

namespace JULEA
{
	class Semantics : public RefCounted<Semantics>
	{
		public:
			Semantics ();
		private:
			~Semantics ();
	};
}

#endif
