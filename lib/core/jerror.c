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
#include <core/jerror.h>

GQuark
julea_backend_error_quark(void)
{
	return g_quark_from_static_string("julea-backend-error-quark");
}
GQuark
julea_frontend_error_quark(void)
{
	return g_quark_from_static_string("julea-frontend-error-quark");
}
#define JULEA_REGISTER_BACKEND_ERROR(e, s) s,
const char* const JuleaBackendErrorFormat[] = {
	"Generic Backend Error%s",
#include <core/jerror.h>
};
#undef JULEA_REGISTER_BACKEND_ERROR
#define JULEA_REGISTER_FRONTEND_ERROR(e, s) s,
const char* const JuleaFrontendErrorFormat[] = {
	"Generic Frontend Error%s",
#include <core/jerror.h>
};
#undef JULEA_REGISTER_FRONTEND_ERROR
