/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2017 Michael Kuhn
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

#include "juleafs.h"

#include <errno.h>

int jfs_utimens (char const* path, const struct timespec ts[2])
{
	JURI* uri;
	int ret = -ENOENT;

	(void)ts;

	if ((uri = jfs_get_uri(path)) == NULL)
	{
		goto end;
	}

	if (!j_uri_get(uri, NULL))
	{
		goto end;
	}

	if (j_uri_get_item(uri) != NULL)
	{
		//guint64 time_;

		//time_ = (ts[1].tv_sec * G_USEC_PER_SEC) + (ts[1].tv_nsec / 1000);

		/* FIXME */
		//j_item_set_modification_time(j_uri_get_item(uri), time_);

		ret = 0;
	}

end:
	if (uri != NULL)
	{
		j_uri_free(uri);
	}

	return ret;
}
