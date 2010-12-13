#include <glib.h>

#include "semantics.h"

struct JSemantics
{
	gint consistency;
	gint persistency;
	gint concurrency;
	gint security;

	guint ref_count;
};

JSemantics*
j_semantics_new (void)
{
	JSemantics* semantics;

	semantics = g_new(JSemantics, 1);
	semantics->concurrency = J_SEMANTICS_CONCURRENCY_STRICT;
	semantics->consistency = J_SEMANTICS_CONSISTENCY_STRICT;
	semantics->persistency = J_SEMANTICS_PERSISTENCY_STRICT;
	semantics->security = J_SEMANTICS_SECURITY_STRICT;
	semantics->ref_count = 1;

	return semantics;
}

JSemantics*
j_semantics_ref (JSemantics* semantics)
{
	semantics->ref_count++;

	return semantics;
}

void
j_semantics_unref (JSemantics* semantics)
{
	semantics->ref_count--;

	if (semantics->ref_count == 0)
	{
		g_free(semantics);
	}
}

void
j_semantics_set (JSemantics* semantics, gint key, gint value)
{
	if (key == J_SEMANTICS_CONCURRENCY)
	{
		semantics->concurrency = value;
	}
	else if (key == J_SEMANTICS_CONSISTENCY)
	{
		semantics->consistency = value;
	}
	else if (key == J_SEMANTICS_PERSISTENCY)
	{
		semantics->persistency = value;
	}
	else if (key == J_SEMANTICS_SECURITY)
	{
		semantics->security = value;
	}
}

gint
j_semantics_get (JSemantics* semantics, gint key)
{
	if (key == J_SEMANTICS_CONCURRENCY)
	{
		return semantics->concurrency;
	}
	else if (key == J_SEMANTICS_CONSISTENCY)
	{
		return semantics->consistency;
	}
	else if (key == J_SEMANTICS_PERSISTENCY)
	{
		return semantics->persistency;
	}
	else if (key == J_SEMANTICS_SECURITY)
	{
		return semantics->security;
	}
}
