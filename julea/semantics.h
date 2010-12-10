#ifndef H_SEMANTICS
#define H_SEMANTICS

/*
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
			Strict,
			Lax
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

		friend class _Store;

		public:
			Consistency::Type const& GetConsistency () const;
			_Semantics* SetConsistency (Consistency::Type);
			Persistency::Type const& GetPersistency () const;
			_Semantics* SetPersistency (Persistency::Type);
			Concurrency::Type const& GetConcurrency () const;
			_Semantics* SetConcurrency (Concurrency::Type);
			Security::Type const& GetSecurity () const;
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
*/

#endif
