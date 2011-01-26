/*
 * Copyright (c) 2010-2011 Michael Kuhn
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

/**
 * \file
 **/

#include <glib.h>

#include "jsemantics.h"

/**
 * \defgroup JSemantics Semantics
 * @{
 **/

/**
 * A semantics object.
 **/
struct JSemantics
{
	/**
	 * The consistency semantics.
	 **/
	gint consistency;

	/**
	 * The persistency semantics.
	 **/
	gint persistency;

	/**
	 * The concurrency semantics.
	 **/
	gint concurrency;

	/**
	 * The security semantics.
	 **/
	gint security;

	/**
	 * The reference count.
	 **/
	guint ref_count;
};

/**
 * Creates a new semantics object.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \return A new semantics object. Should be freed with j_semantics_unref().
 **/
JSemantics*
j_semantics_new (void)
{
	JSemantics* semantics;

	semantics = g_slice_new(JSemantics);
	semantics->concurrency = J_SEMANTICS_CONCURRENCY_STRICT;
	semantics->consistency = J_SEMANTICS_CONSISTENCY_STRICT;
	semantics->persistency = J_SEMANTICS_PERSISTENCY_STRICT;
	semantics->security = J_SEMANTICS_SECURITY_STRICT;
	semantics->ref_count = 1;

	return semantics;
}

/**
 * Increases the semantics' reference count.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param semantics The semantics.
 *
 * \return The semantics.
 **/
JSemantics*
j_semantics_ref (JSemantics* semantics)
{
	g_return_val_if_fail(semantics != NULL, NULL);

	semantics->ref_count++;

	return semantics;
}

/**
 * Decreases the semantics' reference count.
 * When the reference count reaches zero, frees the memory allocated for the semantics.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param semantics The semantics.
 **/
void
j_semantics_unref (JSemantics* semantics)
{
	g_return_if_fail(semantics != NULL);

	semantics->ref_count--;

	if (semantics->ref_count == 0)
	{
		g_slice_free(JSemantics, semantics);
	}
}

/**
 * Sets a specific aspect of the semantics.
 *
 * \author Michael Kuhn
 *
 * \code
 * JSemantics* semantics;
 * ...
 * j_semantics_set(semantics, J_SEMANTICS_PERSISTENCY, J_SEMANTICS_PERSISTENCY_LAX);
 * \endcode
 *
 * \param semantics The semantics.
 * \param key       The aspect's key.
 * \param value     The aspect's value.
 **/
void
j_semantics_set (JSemantics* semantics, gint key, gint value)
{
	g_return_if_fail(semantics != NULL);

	switch (key)
	{
		case J_SEMANTICS_CONCURRENCY:
			semantics->concurrency = value;
			break;
		case J_SEMANTICS_CONSISTENCY:
			semantics->consistency = value;
			break;
		case J_SEMANTICS_PERSISTENCY:
			semantics->persistency = value;
			break;
		case J_SEMANTICS_SECURITY:
			semantics->security = value;
			break;
		default:
			g_warn_if_reached();
	}
}

/**
 * Gets a specific aspect of the semantics.
 *
 * \author Michael Kuhn
 *
 * \code
 * JSemantics* semantics;
 * ...
 * j_semantics_get(semantics, J_SEMANTICS_PERSISTENCY);
 * \endcode
 *
 * \param semantics The semantics.
 * \param key       The aspect's key.
 *
 * \return The aspect's value.
 **/
gint
j_semantics_get (JSemantics* semantics, gint key)
{
	g_return_val_if_fail(semantics != NULL, -1);

	switch (key)
	{
		case J_SEMANTICS_CONCURRENCY:
			return semantics->concurrency;
		case J_SEMANTICS_CONSISTENCY:
			return semantics->consistency;
		case J_SEMANTICS_PERSISTENCY:
			return semantics->persistency;
		case J_SEMANTICS_SECURITY:
			return semantics->security;
		default:
			g_return_val_if_reached(-1);
	}
}

/**
 * @}
 **/
