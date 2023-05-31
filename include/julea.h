/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2023 Michael Kuhn
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

#ifndef JULEA_H
#define JULEA_H

/**
 * JULEA is a distributed file system with a newly designed application programming interface.
 * It also allows to change the file system semantics at runtime.
 *
 * The name JULEA is a play on ROMIO.
 **/

#include <core/jbackend.h>
#include <core/jbackend-operation.h>
#include <core/jbackground-operation.h>
#include <core/jbatch.h>
#include <core/jcache.h>
#include <core/jconfiguration.h>
#include <core/jconnection-pool.h>
#include <core/jcredentials.h>
#include <core/jdir-iterator.h>
#include <core/jdistribution.h>
#include <core/jhelper.h>
#include <core/jlist.h>
#include <core/jlist-iterator.h>
#include <core/jmemory-chunk.h>
#include <core/jmessage.h>
#include <core/jnetwork.h>
#include <core/joperation.h>
#include <core/jsemantics.h>
#include <core/jstatistics.h>
#include <core/jtrace.h>

#undef JULEA_H

#endif
