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

#include "test-db-backend.c"

int
main(int argc, char* argv[])
{
	if (argc > 1)
	{
		test_db_backend_create_base_test_files(argv[1]);
		return 0;
	}
	test_db_backend_init();
#ifdef __AFL_HAVE_MANUAL_CONTROL
	//https://github.com/mirrorer/afl/tree/master/llvm_mode
	// this does not work with threads or network connections
	//        __AFL_INIT();
	//      while (__AFL_LOOP(1000))
#endif
	{
	loop:
		MY_READ_MAX(event, _AFL_EVENT_DB_COUNT);
		MY_READ(random_values);
		test_db_backend_exec();
		goto loop;
	cleanup:
		test_db_backend_cleanup();
	}
	return 0;
}
