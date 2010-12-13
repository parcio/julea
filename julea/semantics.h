#ifndef H_SEMANTICS
#define H_SEMANTICS

struct JSemantics;

typedef struct JSemantics JSemantics;

enum
{
	J_SEMANTICS_CONSISTENCY,
	J_SEMANTICS_PERSISTENCY,
	J_SEMANTICS_CONCURRENCY,
	J_SEMANTICS_SECURITY
};

enum
{
	J_SEMANTICS_CONSISTENCY_STRICT
};

enum
{
	J_SEMANTICS_PERSISTENCY_STRICT,
	J_SEMANTICS_PERSISTENCY_LAX
};

enum
{
	J_SEMANTICS_CONCURRENCY_STRICT
};

enum
{
	J_SEMANTICS_SECURITY_STRICT
};

JSemantics* j_semantics_new (void);
JSemantics* j_semantics_ref (JSemantics*);
void j_semantics_unref (JSemantics*);

void j_semantics_set (JSemantics*, gint, gint);
gint j_semantics_get (JSemantics*, gint);

/*
#include "collection.h"
#include "public.h"
#include "ref_counted.h"

namespace JULEA
{
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
