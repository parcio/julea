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

/**
 * \file
 */

#include <glib.h>

#include <sys/types.h>
#include <unistd.h>

#include "jcredentials.h"

/**
 * \defgroup JCredentials Credentials
 * @{
 */

struct JCredentials
{
	uid_t user;
	uid_t group;

	guint ref_count;
};

JCredentials*
j_credentials_new (void)
{
	JCredentials* credentials;

	credentials = g_new(JCredentials, 1);
	credentials->user = geteuid();
	credentials->group = getegid();
	credentials->ref_count = 1;

	return credentials;
}

JCredentials*
j_credentials_ref (JCredentials* credentials)
{
	g_return_val_if_fail(credentials != NULL, NULL);

	credentials->ref_count++;

	return credentials;
}

void
j_credentials_unref (JCredentials* credentials)
{
	g_return_if_fail(credentials != NULL);

	credentials->ref_count--;

	if (credentials->ref_count == 0)
	{
		g_free(credentials);
	}
}

/*
	Credentials::Credentials (int user, int group)
		: m_user(user), m_group(group)
	{
	}

	int Credentials::User ()
	{
		return m_user;
	}

	int Credentials::Group ()
	{
		return m_group;
	}
*/

/**
 * @}
 */
