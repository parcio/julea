/*
 * Copyright (c) 2010-2012 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>

#include <jsemantics.h>

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
	 * The redundancy semantics.
	 **/
	gint redundancy;

	/**
	 * The security semantics.
	 **/
	gint security;

	/**
	 * Whether the semantics object is immutable.
	 **/
	gboolean immutable;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

/**
 * Creates a new semantics object.
 * Semantics objects become immutable after the first call to j_semantics_ref().
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \return A new semantics object. Should be freed with j_semantics_unref().
 **/
JSemantics*
j_semantics_new (JSemanticsTemplate template)
{
	JSemantics* semantics;

	semantics = g_slice_new(JSemantics);
	semantics->concurrency = J_SEMANTICS_CONCURRENCY_OVERLAPPING;
	semantics->consistency = J_SEMANTICS_CONSISTENCY_IMMEDIATE;
	semantics->persistency = J_SEMANTICS_PERSISTENCY_EVENTUAL;
	semantics->redundancy = J_SEMANTICS_REDUNDANCY_NONE;
	semantics->security = J_SEMANTICS_SECURITY_STRICT;
	semantics->immutable = FALSE;
	semantics->ref_count = 1;

	switch (template)
	{
		case J_SEMANTICS_TEMPLATE_DEFAULT:
			break;
		case J_SEMANTICS_TEMPLATE_POSIX:
			semantics->concurrency = J_SEMANTICS_CONCURRENCY_OVERLAPPING;
			semantics->consistency = J_SEMANTICS_CONSISTENCY_IMMEDIATE;
			semantics->persistency = J_SEMANTICS_PERSISTENCY_EVENTUAL;
			semantics->redundancy = J_SEMANTICS_REDUNDANCY_NONE;
			semantics->security = J_SEMANTICS_SECURITY_STRICT;
			break;
		case J_SEMANTICS_TEMPLATE_CHECKPOINT:
			semantics->concurrency = J_SEMANTICS_CONCURRENCY_NON_OVERLAPPING;
			semantics->consistency = J_SEMANTICS_CONSISTENCY_EVENTUAL;
			semantics->persistency = J_SEMANTICS_PERSISTENCY_ON_CLOSE;
			semantics->redundancy = J_SEMANTICS_REDUNDANCY_NONE;
			semantics->security = J_SEMANTICS_SECURITY_NONE;
			break;
		default:
			g_warn_if_reached();
	}

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

	g_atomic_int_inc(&(semantics->ref_count));

	if (!semantics->immutable)
	{
		semantics->immutable = TRUE;
	}

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

	if (g_atomic_int_dec_and_test(&(semantics->ref_count)))
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
j_semantics_set (JSemantics* semantics, JSemanticsType key, gint value)
{
	g_return_if_fail(semantics != NULL);
	g_return_if_fail(!semantics->immutable);

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
		case J_SEMANTICS_REDUNDANCY:
			semantics->redundancy = value;
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
j_semantics_get (JSemantics* semantics, JSemanticsType key)
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
		case J_SEMANTICS_REDUNDANCY:
			return semantics->redundancy;
		case J_SEMANTICS_SECURITY:
			return semantics->security;
		default:
			g_return_val_if_reached(-1);
	}
}

/**
 * @}
 **/
