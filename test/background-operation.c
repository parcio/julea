/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>

#include <julea.h>

#include <jbackground-operation.h>

#include "test.h"

static gpointer
on_background_operation_completed(gpointer data)
{
	(void)data;

	return NULL;
}

static void
test_background_operation_new_ref_unref(void)
{
	JBackgroundOperation* background_operation;

	background_operation = j_background_operation_new(on_background_operation_completed, NULL);
	j_background_operation_ref(background_operation);
	j_background_operation_unref(background_operation);
	j_background_operation_unref(background_operation);
}

static void
test_background_operation_wait(void)
{
	JBackgroundOperation* background_operation;

	background_operation = j_background_operation_new(on_background_operation_completed, NULL);

	j_background_operation_wait(background_operation);
	j_background_operation_unref(background_operation);
}

void
test_background_operation(void)
{
	g_test_add_func("/background_operation/new_ref_unref", test_background_operation_new_ref_unref);
	g_test_add_func("/background_operation/wait", test_background_operation_wait);
}
