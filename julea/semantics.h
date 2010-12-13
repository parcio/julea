/*
 * Copyright (c) 2010 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

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
