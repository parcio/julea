#ifndef H_SEMANTICS
#define H_SEMANTICS

#include <boost/utility.hpp>

#include <mongo/db/jsobj.h>

namespace JULEA
{
	class _Semantics;
	class Semantics;
}

#include "collection.h"
#include "public.h"
#include "ref_counted.h"

namespace JULEA
{
	namespace Consistency
	{
		enum Type
		{
			Strict
		};
	}

	namespace Persistency
	{
		enum Type
		{
			Strict
		};
	}

	namespace Concurrency
	{
		enum Type
		{
			Strict
		};
	}

	namespace Security
	{
		enum Type
		{
			Strict
		};
	}

	class _Semantics : public RefCounted<_Semantics>
	{
		friend class RefCounted<_Semantics>;

		friend class Semantics;

		public:
			_Semantics* SetConsistency (Consistency::Type);
			_Semantics* SetPersistency (Persistency::Type);
			_Semantics* SetConcurrency (Concurrency::Type);
			_Semantics* SetSecurity (Security::Type);
		private:
			_Semantics ();
			~_Semantics ();

			Consistency::Type m_consistency;
			Persistency::Type m_persistency;
			Concurrency::Type m_concurrency;
			Security::Type m_security;
	};

	class Semantics : public Public<_Semantics>
	{
		friend class _Item;
		friend class _Store;

		public:
			Semantics ()
			{
				m_p = new _Semantics();
			}

		private:
			Semantics (_Semantics* p)
				: Public<_Semantics>(p)
			{
			}
	};
}

#endif
