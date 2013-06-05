/*
 * Copyright (c) 2010-2013 Michael Kuhn
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
 **/

#ifndef H_DISTRIBUTION
#define H_DISTRIBUTION

#include <glib.h>

#include <bson.h>

enum JDistributionType
{
	J_DISTRIBUTION_NONE,
	J_DISTRIBUTION_ROUND_ROBIN,
	J_DISTRIBUTION_SINGLE_SERVER
};

typedef enum JDistributionType JDistributionType;

struct JDistribution;

typedef struct JDistribution JDistribution;

#include <jconfiguration.h>

JDistribution* j_distribution_new (JConfiguration*, JDistributionType);
void j_distribution_free (JDistribution*);

void j_distribution_init (JDistribution*, guint64, guint64);
gboolean j_distribution_distribute (JDistribution*, guint*, guint64*, guint64*, guint64*);

void j_distribution_set_block_size (JDistribution*, guint64);
void j_distribution_set_round_robin_start_index (JDistribution*, guint);
void j_distribution_set_single_server_index (JDistribution*, guint);

bson* j_distribution_serialize (JDistribution*);
void j_distribution_deserialize (JDistribution*, bson const*);

#endif
