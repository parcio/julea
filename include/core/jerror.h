/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2019 Benjamin Warnke
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

#ifndef JULEA_ERROR_H
#define JULEA_ERROR_H

#include <glib.h>

#define JULEA_REGISTER_BACKEND_ERROR(e, s) e,
enum JuleaBackendError
{
	JULEA_BACKEND_ERROR_FAILED = 0,
#include <core/jerror.h>
	_JULEA_BACKEND_ERROR_COUNT,
};
#undef JULEA_REGISTER_BACKEND_ERROR
extern const char* const JuleaBackendErrorFormat[];

#define JULEA_REGISTER_FRONTEND_ERROR(e, s) e,
enum JuleaFrontendError
{
	JULEA_FRONTEND_ERROR_FAILED = 0,
#include <core/jerror.h>
	_JULEA_FRONTEND_ERROR_COUNT,
};
#undef JULEA_REGISTER_FRONTEND_ERROR
extern const char* const JuleaFrontendErrorFormat[];

#define JULEA_BACKEND_ERROR julea_backend_error_quark()
GQuark julea_backend_error_quark(void);
#define j_goto_error_backend(val, err_code, ...)                                                                                                  \
	do                                                                                                                                        \
	{                                                                                                                                         \
		_Pragma("GCC diagnostic push");                                                                                                   \
		_Pragma("GCC diagnostic ignored \"-Wformat-nonliteral\"");                                                                        \
		if (val)                                                                                                                          \
		{                                                                                                                                 \
			J_DEBUG_ERROR(JuleaBackendErrorFormat[err_code], ##__VA_ARGS__);                                                          \
			g_set_error(error, JULEA_BACKEND_ERROR, err_code, JuleaBackendErrorFormat[err_code], G_STRLOC, G_STRFUNC, ##__VA_ARGS__); \
			goto _error;                                                                                                              \
		}                                                                                                                                 \
		_Pragma("GCC diagnostic pop");                                                                                                    \
	} while (0)
#define JULEA_FRONTEND_ERROR julea_frontend_error_quark()
GQuark julea_frontend_error_quark(void);
#define j_goto_error_frontend(val, err_code, ...)                                                                                                   \
	do                                                                                                                                          \
	{                                                                                                                                           \
		_Pragma("GCC diagnostic push");                                                                                                     \
		_Pragma("GCC diagnostic ignored \"-Wformat-nonliteral\"");                                                                          \
		if (val)                                                                                                                            \
		{                                                                                                                                   \
			J_DEBUG_ERROR(JuleaFrontendErrorFormat[err_code], ##__VA_ARGS__);                                                           \
			g_set_error(error, JULEA_FRONTEND_ERROR, err_code, JuleaFrontendErrorFormat[err_code], G_STRLOC, G_STRFUNC, ##__VA_ARGS__); \
			goto _error;                                                                                                                \
		}                                                                                                                                   \
		_Pragma("GCC diagnostic pop");                                                                                                      \
	} while (0)

#define j_goto_error_subcommand(val) \
	do                           \
	{                            \
		if (val)             \
			goto _error; \
	} while (0)
#endif

#ifdef JULEA_REGISTER_BACKEND_ERROR
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_BATCH_NULL, "%s:%s: batch not set%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_BSON_APPEND_FAILED, "%s:%s: bson append failed. db type was '%s'")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_BSON_FAILED, "%s:%s: bson generic error")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_BSON_INVALID, "%s:%s: bson invalid%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_BSON_INVALID_TYPE, "%s:%s: bson invalid type '%d'")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_BSON_ITER_INIT, "%s:%s: bson iter init failed%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_BSON_ITER_RECOURSE, "%s:%s: bson iter recourse failed%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_BSON_KEY_NOT_FOUND, "%s:%s: bson is missing required key '%s'")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_COMPARATOR_INVALID, "%s:%s: db comparator invalid '%d'")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_DB_TYPE_INVALID, "%s:%s: db invalid type '%d'")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_ITERATOR_NO_MORE_ELEMENTS, "%s:%s: no more elements to iterate%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_ITERATOR_NULL, "%s:%s: iterator not set%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_METADATA_EMPTY, "%s:%s: metadata is empty%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_METADATA_NULL, "%s:%s: metadata not set%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_NAME_NULL, "%s:%s: name not set%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_NAMESPACE_NULL, "%s:%s: namespace not set%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_NO_VARIABLE_SET, "%s:%s: no variable set to a not NULL value%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_OPERATOR_INVALID, "%s:%s: db operator invalid '%d'")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_SCHEMA_EMPTY, "%s:%s: schema is empty%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_SCHEMA_NOT_FOUND, "%s:%s: schema not found%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_SCHEMA_NULL, "%s:%s: schema not set%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_SELECTOR_EMPTY, "%s:%s: selector is empty%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_SELECTOR_NULL, "%s:%s: selector not set%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_SQL_CONSTRAINT, "%s:%s: sql constraint error '%d' '%s'")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_SQL_FAILED, "%s:%s: sql error '%d' '%s'")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_THREADING_ERROR, "%s:%s: some other thread modified critical variables without lock%s")
JULEA_REGISTER_BACKEND_ERROR(JULEA_BACKEND_ERROR_VARIABLE_NOT_FOUND, "%s:%s: variable '%s' not defined in schema")
#endif
#ifdef JULEA_REGISTER_FRONTEND_ERROR
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_BATCH_NULL, "%s:%s: batch not set%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_BSON_APPEND_FAILED, "%s:%s: bson append failed. db type was '%s'")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_BSON_INITIALIZED, "%s:%s: bson initialized%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_BSON_INVALID_TYPE, "%s:%s: bson invalid type '%d'")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_BSON_ITER_INIT, "%s:%s: bson iter init failed%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_BSON_KEY_FOUND, "%s:%s: bson already contains key '%s'")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_BSON_NOT_INITIALIZED, "%s:%s: bson not initialized%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_DB_BSON_SERVER, "%s:%s: bson stored on server - no modifications, no create again, no load again%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_DB_TYPE_INVALID, "%s:%s: db invalid type '%d'")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_ENTRY_NULL, "%s:%s: entry not set%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_ITERATOR_NO_MORE_ELEMENTS, "%s:%s: no more elements to iterate%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_ITERATOR_NULL, "%s:%s: iterator not set%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_LENGTH_NULL, "%s:%s: length not set%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_NAME_NULL, "%s:%s: name not set%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_NAMESPACE_NULL, "%s:%s: namespace not set%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_OPERATOR_INVALID, "%s:%s: db operator invalid '%d'")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_SCHEMA_NULL, "%s:%s: schema not set%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_SELECTOR_EMPTY, "%s:%s: selector empty%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_SELECTOR_EQUAL, "%s:%s: selector must not equal %s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_SELECTOR_MODE_INVALID, "%s:%s: selector mode invalid. mode was '%d'")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_SELECTOR_NULL, "%s:%s: selector not set%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_SELECTOR_TOO_COMPLEX, "%s:%s: selector must not contain more than '%d' variables")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_TYPE_NULL, "%s:%s: type not set%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_VALUE_NULL, "%s:%s: value not set%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_VARIABLE_ALREADY_SET, "%s:%s: variable already set%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_VARIABLE_NAME_NULL, "%s:%s: variable name not set%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_VARIABLE_NOT_FOUND, "%s:%s: variable not found%s")
JULEA_REGISTER_FRONTEND_ERROR(JULEA_FRONTEND_ERROR_VARIABLE_TYPE_NULL, "%s:%s: variable type not set%s")
#endif
