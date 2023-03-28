/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2022 Michael Kuhn
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <jsemantics.h>
#include <jtrace.h>

/**
 * \addtogroup JSemantics Semantics
 *
 * @{
 **/

/**
 * A semantics object.
 **/
struct JSemantics
{
	gint atomicity;

	/**
	 * The consistency semantics.
	 **/
	JSemanticsConsistency consistency;

	/**
	 * The persistency semantics.
	 **/
	JSemanticsPersistency persistency;

	/**
	 * The security semantics.
	 **/
	JSemanticsSecurity security;

	/**
	 * Whether the semantics object is immutable.
	 **/
	gboolean immutable;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};

enum JSerializedSemantics
{
	J_SERIALIZED_SEMANTICS_ATOMICITY_BATCH = 1 << 0,
	J_SERIALIZED_SEMANTICS_ATOMICITY_OPERATION = 1 << 1,
	J_SERIALIZED_SEMANTICS_ATOMICITY_NONE = 1 << 2,
	J_SERIALIZED_SEMANTICS_CONSISTENCY_IMMEDIATE = 1 << 3,
	J_SERIALIZED_SEMANTICS_CONSISTENCY_SESSION = 1 << 4,
	J_SERIALIZED_SEMANTICS_CONSISTENCY_EVENTUAL = 1 << 5,
	J_SERIALIZED_SEMANTICS_PERSISTENCY_STORAGE = 1 << 6,
	J_SERIALIZED_SEMANTICS_PERSISTENCY_NETWORK = 1 << 7,
	J_SERIALIZED_SEMANTICS_PERSISTENCY_NONE = 1 << 8,
	J_SERIALIZED_SEMANTICS_SECURITY_STRICT = 1 << 9,
	J_SERIALIZED_SEMANTICS_SECURITY_NONE = 1 << 10
};

typedef enum JSerializedSemantics JSerializedSemantics;

JSemantics*
j_semantics_new(JSemanticsTemplate template)
{
	J_TRACE_FUNCTION(NULL);

	JSemantics* semantics;

	semantics = g_slice_new(JSemantics);
	semantics->atomicity = J_SEMANTICS_ATOMICITY_NONE;
	semantics->consistency = J_SEMANTICS_CONSISTENCY_IMMEDIATE;
	semantics->persistency = J_SEMANTICS_PERSISTENCY_NETWORK;
	semantics->security = J_SEMANTICS_SECURITY_NONE;
	semantics->immutable = FALSE;
	semantics->ref_count = 1;

	switch (template)
	{
		case J_SEMANTICS_TEMPLATE_DEFAULT:
			break;
		case J_SEMANTICS_TEMPLATE_POSIX:
			semantics->atomicity = J_SEMANTICS_ATOMICITY_OPERATION;
			semantics->consistency = J_SEMANTICS_CONSISTENCY_IMMEDIATE;
			semantics->persistency = J_SEMANTICS_PERSISTENCY_NETWORK;
			semantics->security = J_SEMANTICS_SECURITY_STRICT;
			break;
		case J_SEMANTICS_TEMPLATE_TEMPORARY_LOCAL:
			semantics->atomicity = J_SEMANTICS_ATOMICITY_NONE;
			semantics->consistency = J_SEMANTICS_CONSISTENCY_EVENTUAL;
			semantics->persistency = J_SEMANTICS_PERSISTENCY_NETWORK;
			semantics->security = J_SEMANTICS_SECURITY_NONE;
			break;
		default:
			g_warn_if_reached();
	}

	return semantics;
}

JSemantics*
j_semantics_new_from_string(gchar const* template_str, gchar const* semantics_str)
{
	J_TRACE_FUNCTION(NULL);

	JSemantics* semantics = NULL;
	g_auto(GStrv) parts = NULL;
	guint parts_len;

	if (template_str == NULL || g_strcmp0(template_str, "default") == 0)
	{
		semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
	}
	else if (g_strcmp0(template_str, "posix") == 0)
	{
		semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_POSIX);
	}
	else if (g_strcmp0(template_str, "temporary-local") == 0)
	{
		semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_TEMPORARY_LOCAL);
	}
	else
	{
		g_assert_not_reached();
	}

	if (semantics_str == NULL)
	{
		return semantics;
	}

	parts = g_strsplit(semantics_str, ",", 0);
	parts_len = g_strv_length(parts);

	for (guint i = 0; i < parts_len; i++)
	{
		gchar const* value;

		if ((value = strchr(parts[i], '=')) == NULL)
		{
			continue;
		}

		value++;

		if (g_str_has_prefix(parts[i], "atomicity="))
		{
			if (g_strcmp0(value, "batch") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_ATOMICITY, J_SEMANTICS_ATOMICITY_BATCH);
			}
			else if (g_strcmp0(value, "operation") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_ATOMICITY, J_SEMANTICS_ATOMICITY_OPERATION);
			}
			else if (g_strcmp0(value, "none") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_ATOMICITY, J_SEMANTICS_ATOMICITY_NONE);
			}
			else
			{
				g_assert_not_reached();
			}
		}
		else if (g_str_has_prefix(parts[i], "consistency="))
		{
			if (g_strcmp0(value, "immediate") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_CONSISTENCY, J_SEMANTICS_CONSISTENCY_IMMEDIATE);
			}
			else if (g_strcmp0(value, "session") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_CONSISTENCY, J_SEMANTICS_CONSISTENCY_SESSION);
			}
			else if (g_strcmp0(value, "eventual") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_CONSISTENCY, J_SEMANTICS_CONSISTENCY_EVENTUAL);
			}
			else
			{
				g_assert_not_reached();
			}
		}
		else if (g_str_has_prefix(parts[i], "persistency="))
		{
			if (g_strcmp0(value, "storage") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_PERSISTENCY, J_SEMANTICS_PERSISTENCY_STORAGE);
			}
			else if (g_strcmp0(value, "network") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_PERSISTENCY, J_SEMANTICS_PERSISTENCY_NETWORK);
			}
			else if (g_strcmp0(value, "none") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_PERSISTENCY, J_SEMANTICS_PERSISTENCY_NONE);
			}
			else
			{
				g_assert_not_reached();
			}
		}
		else if (g_str_has_prefix(parts[i], "security="))
		{
			if (g_strcmp0(value, "strict") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_SECURITY, J_SEMANTICS_SECURITY_STRICT);
			}
			else if (g_strcmp0(value, "none") == 0)
			{
				j_semantics_set(semantics, J_SEMANTICS_SECURITY, J_SEMANTICS_SECURITY_NONE);
			}
			else
			{
				g_assert_not_reached();
			}
		}
		else
		{
			g_assert_not_reached();
		}
	}

	return semantics;
}

JSemantics*
j_semantics_ref(JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(semantics != NULL, NULL);

	g_atomic_int_inc(&(semantics->ref_count));

	if (!semantics->immutable)
	{
		semantics->immutable = TRUE;
	}

	return semantics;
}

void
j_semantics_unref(JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(semantics != NULL);

	if (g_atomic_int_dec_and_test(&(semantics->ref_count)))
	{
		g_slice_free(JSemantics, semantics);
	}
}

void
j_semantics_set(JSemantics* semantics, JSemanticsType key, gint value)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(semantics != NULL);
	g_return_if_fail(!semantics->immutable);

	switch (key)
	{
		case J_SEMANTICS_ATOMICITY:
			semantics->atomicity = value;
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

gint
j_semantics_get(JSemantics* semantics, JSemanticsType key)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(semantics != NULL, -1);

	switch (key)
	{
		case J_SEMANTICS_ATOMICITY:
			return semantics->atomicity;
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

guint32
j_semantics_serialize(const JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	guint32 serialized_semantics = 0;

	g_return_val_if_fail(semantics != NULL, 0);

#define SERIALIZE_SEMANTICS(type, key) \
	{ \
		gint tmp; \
		tmp = j_semantics_get(semantics, J_SEMANTICS_##type); \
		if (tmp == J_SEMANTICS_##type##_##key) \
		{ \
			serialized_semantics |= J_SERIALIZED_SEMANTICS_##type##_##key; \
		} \
	}

	SERIALIZE_SEMANTICS(ATOMICITY, BATCH)
	SERIALIZE_SEMANTICS(ATOMICITY, OPERATION)
	SERIALIZE_SEMANTICS(ATOMICITY, NONE)
	SERIALIZE_SEMANTICS(CONSISTENCY, IMMEDIATE)
	SERIALIZE_SEMANTICS(CONSISTENCY, SESSION)
	SERIALIZE_SEMANTICS(CONSISTENCY, EVENTUAL)
	SERIALIZE_SEMANTICS(PERSISTENCY, STORAGE)
	SERIALIZE_SEMANTICS(PERSISTENCY, NETWORK)
	SERIALIZE_SEMANTICS(PERSISTENCY, NONE)
	SERIALIZE_SEMANTICS(SECURITY, STRICT)
	SERIALIZE_SEMANTICS(SECURITY, NONE)

#undef SERIALIZE_SEMANTICS
	return GUINT32_TO_LE(serialized_semantics);
}

JSemantics*
j_semantics_deserialize(guint32 serial_semantics)
{
	J_TRACE_FUNCTION(NULL);

	JSemantics* semantics;
	serial_semantics = GUINT32_FROM_LE(serial_semantics);

	// semantics == 0, will lead to the default template
	semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);

#define DESERIALIZE_SEMANTICS(type, key) \
	if (serial_semantics & J_SERIALIZED_SEMANTICS_##type##_##key) \
	{ \
		j_semantics_set(semantics, J_SEMANTICS_##type, J_SEMANTICS_##type##_##key); \
	}

	DESERIALIZE_SEMANTICS(ATOMICITY, BATCH)
	DESERIALIZE_SEMANTICS(ATOMICITY, OPERATION)
	DESERIALIZE_SEMANTICS(ATOMICITY, NONE)
	DESERIALIZE_SEMANTICS(CONSISTENCY, IMMEDIATE)
	DESERIALIZE_SEMANTICS(CONSISTENCY, SESSION)
	DESERIALIZE_SEMANTICS(CONSISTENCY, EVENTUAL)
	DESERIALIZE_SEMANTICS(PERSISTENCY, STORAGE)
	DESERIALIZE_SEMANTICS(PERSISTENCY, NETWORK)
	DESERIALIZE_SEMANTICS(PERSISTENCY, NONE)
	DESERIALIZE_SEMANTICS(SECURITY, STRICT)
	DESERIALIZE_SEMANTICS(SECURITY, NONE)

#undef DESERIALIZE_SEMANTICS

	return semantics;
}

/**
 * @}
 **/
