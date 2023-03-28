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

#ifndef JULEA_SEMANTICS_H
#define JULEA_SEMANTICS_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

/**
 * \defgroup JSemantics Semantics
 *
 * @{
 **/

enum JSemanticsTemplate
{
	J_SEMANTICS_TEMPLATE_DEFAULT,
	J_SEMANTICS_TEMPLATE_POSIX,
	J_SEMANTICS_TEMPLATE_TEMPORARY_LOCAL
};

typedef enum JSemanticsTemplate JSemanticsTemplate;

/**
 * Semantics accepted by JULEA.
 */
enum JSemanticsType
{
	/**
	 * Atomicity of batched operations.
	 *
	 * \attention Currently only database functions are affected by this setting.
	 */
	J_SEMANTICS_ATOMICITY,

	/**
	 * Defines when data will be consistent for other clients, that is, when batches are executed.
	 */
	J_SEMANTICS_CONSISTENCY,

	/**
	 * Defines which guarantees are given on the persistency of finished operations.
	 */
	J_SEMANTICS_PERSISTENCY,

	/**
	 * Defines the used security model.
	 *
	 * \attention Currently unused.
	 */
	J_SEMANTICS_SECURITY
};

typedef enum JSemanticsType JSemanticsType;

/**
 * Atomicity of batched operations.
 *
 * \attention Currently only database functions are affected by this setting.
 */
enum JSemanticsAtomicity
{
	/**
	 * Consecutive operations of the same type are part of the same DB transaction.
	 *
	 * \todo Generalize to complete batch instead of same lists.
	 */
	J_SEMANTICS_ATOMICITY_BATCH,

	/**
	 * Each operation is a transaction.
	 */
	J_SEMANTICS_ATOMICITY_OPERATION,

	/**
	 * No transactions are used.
	 *
	 * \todo Currently unused. Interesting when other backends support transactions.
	 */
	J_SEMANTICS_ATOMICITY_NONE
};

typedef enum JSemanticsAtomicity JSemanticsAtomicity;

/**
 * Consistency levels describe when other clients are able to read written data.
 *
 * The implemented levels are loosely based on the levels described in:
 *
 * Chen Wang, Kathryn Mohror, and Marc Snir. 2021. File System Semantics Requirements of HPC Applications.
 * In Proceedings of the 30th International Symposium on High-Performance Parallel and Distributed Computing (HPDC '21).
 * Association for Computing Machinery, New York, NY, USA, 19–30. https://doi.org/10.1145/3431379.3460637
 *
 * \attention Mixing immediate/eventual consistencies with session-based consistency for concurrently executed batches gives no guarantees on local consistency.
 */
enum JSemanticsConsistency
{
	/**
	 * Data is consistent immediately after the batch is executed.
	 */
	J_SEMANTICS_CONSISTENCY_IMMEDIATE,

	/**
	 * Data is consistent immediately after no reference to the batch object is held anymore.
	 */
	J_SEMANTICS_CONSISTENCY_SESSION,

	/**
	 * The data of a corresponding batch will be eventually consistent after calling j_batch_execute().
	 * Current implicit synchronization points are manual execution of any immediate batch, reading operations or program termination.
	 * If more control of asynchronous execution is wanted, consider using j_batch_execute_async() instead of consistency levels.
	 */
	J_SEMANTICS_CONSISTENCY_EVENTUAL
};

typedef enum JSemanticsConsistency JSemanticsConsistency;

/**
 * Defines which guarantees are given on the persistency of finished operations.
 */
enum JSemanticsPersistency
{
	/**
	 * Data of finished operations is stored on persistent storage.
	 */
	J_SEMANTICS_PERSISTENCY_STORAGE,

	/**
	 * Data of finished operations arrived at a JULEA server.
	 */
	J_SEMANTICS_PERSISTENCY_NETWORK,

	/**
	 * No guarantees on persistency.
	 */
	J_SEMANTICS_PERSISTENCY_NONE
};

typedef enum JSemanticsPersistency JSemanticsPersistency;

/**
 * Defines the used security model.
 *
 * \attention Currently unused.
 */
enum JSemanticsSecurity
{
	J_SEMANTICS_SECURITY_STRICT,
	J_SEMANTICS_SECURITY_NONE
};

typedef enum JSemanticsSecurity JSemanticsSecurity;

struct JSemantics;

typedef struct JSemantics JSemantics;

/**
 * Creates a new semantics object.
 * Semantics objects become immutable after the first call to @ref j_semantics_ref().
 *
 * \code
 * \endcode
 * \param template_ A semantics template.
 *
 * \return A new semantics object. Should be freed with @ref j_semantics_unref().
 **/
JSemantics* j_semantics_new(JSemanticsTemplate template_);

/**
 * Creates a new semantics object from two strings.
 * Semantics objects become immutable after the first call to @ref j_semantics_ref().
 *
 * \code
 * JSemantics* semantics;
 *
 * semantics = j_semantics_new_from_string("default", "atomicity=operation");
 * \endcode
 *
 * \param template_str  A string specifying the template.
 * \param semantics_str A string specifying the semantics.
 *
 * \return A new semantics object. Should be freed with @ref j_semantics_unref().
 **/
JSemantics* j_semantics_new_from_string(gchar const* template_str, gchar const* semantics_str);

/**
 * Increases the semantics' reference count.
 * The semantics object becomes immutable after the first call to this.
 *
 * \code
 * \endcode
 *
 * \param semantics The semantics.
 *
 * \return The semantics.
 **/
JSemantics* j_semantics_ref(JSemantics* semantics);

/**
 * Decreases the semantics' reference count.
 * When the reference count reaches zero, frees the memory allocated for the semantics.
 *
 * \code
 * \endcode
 *
 * \param semantics The semantics.
 **/
void j_semantics_unref(JSemantics* semantics);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JSemantics, j_semantics_unref)

/**
 * Sets a specific aspect of the semantics.
 *
 * \code
 * JSemantics* semantics;
 * ...
 * j_semantics_set(semantics, J_SEMANTICS_CONSISTENCY, J_SEMANTICS_CONSISTENCY_SESSION);
 * \endcode
 *
 * \param semantics The semantics.
 * \param key       The aspect's key.
 * \param value     The aspect's value.
 **/
void j_semantics_set(JSemantics* semantics, JSemanticsType key, gint value);

/**
 * Gets a specific aspect of the semantics.
 *
 * \code
 * JSemantics* semantics;
 * ...
 * j_semantics_get(semantics, J_SEMANTICS_CONSISTENCY);
 * \endcode
 *
 * \param semantics The semantics.
 * \param key       The aspect's key.
 *
 * \return The aspect's value.
 **/
gint j_semantics_get(JSemantics* semantics, JSemanticsType key);

/**
 * Encodes semantics in a guint32
 *
 * \code
 * JSemantics* semantics;
 * ...
 * guint32 serial_semantics = j_semantics_serialize(semantics);
 * JSemantics* simular_semantics = j_semantics_deserialize(serial_semantics);
 * \encode
 *
 * \param semnatics The Semantics
 *
 * \return serialized semantics as LE encoded guint32
 * \retval 0 if no semantics are given eg (semantics == NULL)
 **/
guint32 j_semantics_serialize(const JSemantics* semantics);

/**
 * Decode serialized semantics, and create new Semantics
 *
 * For a example see j_semantics_serialize()
 *
 * \param serial_semantics serialized semantics, created with j_semantics_serialize()
 *
 * \return new created JSemantics
 **/
JSemantics* j_semantics_deserialize(guint32 serial_semantics);

/**
 * @}
 **/

G_END_DECLS

#endif
