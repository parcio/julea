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
#if (GLIB_MAJOR_VERSION < 2) || (GLIB_MINOR_VERSION < 58)
#define G_APPROX_VALUE(a, b, epsilon) ((((a) > (b) ? (a) - (b) : (b) - (a)) < (epsilon)) || !isfinite(a) || !isfinite(b))
#endif
#define J_AFL_DEBUG_BSON(bson)                           \
	do                                               \
	{                                                \
		char* json = NULL;                       \
		if (bson)                                \
			json = bson_as_json(bson, NULL); \
		J_DEBUG("json = %s", json);              \
		bson_free((void*)json);                  \
	} while (0)
#define J_AFL_DEBUG_ERROR(ret, ret_expected, error)                                  \
	do                                                                           \
	{                                                                            \
		if (error)                                                           \
		{                                                                    \
			J_DEBUG("ERROR (%d) (%s)", error->code, error->message);     \
		}                                                                    \
		if ((ret) != (ret_expected))                                         \
		{                                                                    \
			J_DEBUG("not expected %d != %d", (ret), (ret_expected));     \
			MYABORT();                                                   \
		}                                                                    \
		if ((ret) != ((error) == NULL))                                      \
		{                                                                    \
			J_DEBUG("not initialized %d != %d", (ret), (error) == NULL); \
			MYABORT();                                                   \
		}                                                                    \
		if (error)                                                           \
		{                                                                    \
			g_error_free(error);                                         \
			error = NULL;                                                \
		}                                                                    \
	} while (0)
#define J_AFL_DEBUG_ERROR_NO_EXPECT(ret, error)                                      \
	do                                                                           \
	{                                                                            \
		if (error)                                                           \
		{                                                                    \
			J_DEBUG("ERROR (%d) (%s)", (error)->code, (error)->message); \
		}                                                                    \
		if ((ret) != ((error) == NULL))                                      \
		{                                                                    \
			J_DEBUG("not initialized %d != %d", (ret), (error) == NULL); \
			MYABORT();                                                   \
		}                                                                    \
		if (error)                                                           \
		{                                                                    \
			g_error_free(error);                                         \
			error = NULL;                                                \
		}                                                                    \
	} while (0)
#define MY_READ(var)                                                              \
	do                                                                        \
	{                                                                         \
		if (read(STDIN_FILENO, &var, sizeof(var)) < (ssize_t)sizeof(var)) \
			goto cleanup;                                             \
	} while (0)
#define MY_READ_LEN(var, len)                                    \
	do                                                       \
	{                                                        \
		if (read(STDIN_FILENO, var, len) < (ssize_t)len) \
			goto cleanup;                            \
	} while (0)
#define MY_READ_MAX(var, max)      \
	do                         \
	{                          \
		MY_READ(var);      \
		var = var % (max); \
	} while (0)
#define MYABORT()                       \
	do                              \
	{                               \
		J_DEBUG("abort %d", 0); \
		abort();                \
	} while (0)
