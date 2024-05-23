/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2024 Michael Kuhn
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

#include <string.h>

#include <julea.h>
#include <julea-object.h>

#include "benchmark.h"

#define NUM_SIZES 3

#define NUM_RW_RATIOS 5

#define READ_FLAG 0

#define WRITE_FLAG 1

#define min(x, y) (((x) <= (y)) ? (x) : (y))

static guint index = 0;

static guint rw_index = 0;

static const guint BLOCK_SIZES[NUM_SIZES] = { 1024, 2 * 1024, 4 * 1024 };

static const gdouble RW_RATIOS[NUM_RW_RATIOS] = { 0.0, 0.25, 0.5, 0.75, 1.0 };

static const guint N = 1000;

static void
_benchmark_object_read_write(BenchmarkRun* run, gboolean use_batch, guint* pattern, guint* rw_pattern, guint block_size, guint n)
{
	g_autoptr(JObject) object = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	g_autofree gchar* dummy = NULL;
	guint64 nb = 0;
	gboolean ret;
	guint rcounter = 0, wcounter = 0;

	dummy = g_malloc0(block_size);

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	object = j_object_new("benchmark", "benchmark");
	j_object_create(object, batch);

	for (guint i = 0; i < n; i++)
	{
		j_object_write(object, dummy, block_size, i * block_size, &nb, batch);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);
	g_assert_cmpuint(nb, ==, n * block_size);

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			// decide read or write
			if (rw_pattern[i] == 0)
			{
				j_object_read(object, dummy, block_size, pattern[i] * block_size, &nb, batch);
				rcounter++;
			}
			else
			{
				j_object_write(object, dummy, block_size, pattern[i] * block_size, &nb, batch);
				wcounter++;
			}

			if (!use_batch)
			{
				ret = j_batch_execute(batch);
				g_assert_true(ret);
				g_assert_cmpuint(nb, ==, block_size);
			}
		}

		if (use_batch)
		{
			ret = j_batch_execute(batch);
			g_assert_true(ret);
			g_assert_cmpuint(nb, ==, n * block_size);
		}
	}

	j_benchmark_timer_stop(run);

	j_object_delete(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	run->operations = n;
	run->bytes = n * block_size;
}

static void
_benchmark_object_write(BenchmarkRun* run, gboolean use_batch, guint* pattern, guint block_size, guint n)
{
	g_autoptr(JObject) object = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	g_autofree gchar* dummy = NULL;
	guint64 nb = 0;
	gboolean ret;

	dummy = g_malloc0(block_size);

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	object = j_object_new("benchmark", "benchmark");
	j_object_create(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_benchmark_timer_start(run);

	while (j_benchmark_iterate(run))
	{
		for (guint i = 0; i < n; i++)
		{
			j_object_write(object, dummy, block_size, pattern[i] * block_size, &nb, batch);

			if (!use_batch)
			{
				ret = j_batch_execute(batch);
				g_assert_true(ret);
				g_assert_cmpuint(nb, ==, block_size);
			}
		}

		if (use_batch)
		{
			ret = j_batch_execute(batch);
			g_assert_true(ret);
			g_assert_cmpuint(nb, ==, n * block_size);
		}
	}

	j_benchmark_timer_stop(run);

	j_object_delete(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	run->operations = n;
	run->bytes = n * block_size;
}

/*
Fills a buffer of a given length with incrementing values starting at 0.
*/
static void
generate_pattern_seq(guint* buf, guint len)
{
	guint i;
	for (i = 0; i < len; i++)
	{
		buf[i] = i;
	}
}

static void
shuffle(guint* buf, guint len)
{
	// set seed
	srand(42);

	// shuffle
	for (guint i = 0; i < len; i++)
	{
		guint j = rand() % (i + 1);

		guint temp = buf[j];
		buf[j] = buf[i];
		buf[i] = temp;
	}
}

static void
generate_rw_pattern(guint* buf, guint n, gdouble rw_ratio)
{
	guint read_ops = 0;
	if (rw_ratio < 0.0001)
	{
		read_ops = n;
	}
	else if (rw_ratio > 0.9999)
	{
		read_ops = 0;
	}
	else
	{
		read_ops = (int)(rw_ratio * n);
	}

	for (guint i = 0; i < n; i++)
	{
		if (i < read_ops)
		{
			buf[i] = READ_FLAG;
		}
		else
		{
			buf[i] = WRITE_FLAG;
		}
	}

	shuffle(buf, n);
}

/*
Fills a buffer of a given length with the values in the range [0, len) shuffled using a set seed.
*/
static void
generate_pattern_rand(guint* buf, guint len)
{
	// insert sequence
	generate_pattern_seq(buf, len);

	shuffle(buf, len);
}

/*
Calculates the nth harmonic number
`H_N = 𝚺(k=1, N) 1/k`.
*/
static double
calc_harmonic(guint n)
{
	double sum = 0.0;
	for (guint i = 1; i <= n; i++)
	{
		sum += (1.0 / i);
	}

	return sum;
}

/*
Fills a buffer of a given length with values in the range of [0, len]
using a distribution following Zipf's law.

This is realized by adding the same value multiple times
based on the frequency derived from the value's rank.

The range [0, len) serves as a basis with the range being considered to already be ordered.

The resulting range is then shuffled.
*/
static void
generate_pattern_zipf(guint* buf, guint len)
{
	guint j = 0;
	const double HARMONIC = calc_harmonic(len);

	// generate template of randomly ordered indices
	g_autofree guint* template = g_malloc0(len * sizeof(guint));
	generate_pattern_rand(template, len);

	// apply distribution
	for (guint i = 0; i < len; i++)
	{
		double f = (1 / HARMONIC) * (1.0 / (i + 1));

		// always round up to get atleast 1 occurence
		guint n_occurences = (f * len) + 1;

		for (guint k = j; k < min(len, j + n_occurences); k++)
		{
			buf[k] = template[i];
		}

		j += n_occurences;

		if (j >= len)
		{
			break;
		}
	}

	// shuffle distribution
	shuffle(buf, len);
}

static void
benchmark_object_write(BenchmarkRun* run, gboolean use_batch, void (*generator)(guint*, guint))
{
	guint n = (use_batch) ? 10 * N : N;
	guint block_size = BLOCK_SIZES[index];
	g_autofree guint* pattern = g_malloc0(n * sizeof(guint));

	index = (index + 1) % NUM_SIZES;

	(*generator)(pattern, n);

	_benchmark_object_write(run, use_batch, pattern, block_size, n);
}

static void
benchmark_object_read_write(BenchmarkRun* run, gboolean use_batch, void (*generator)(guint*, guint))
{
	guint n = (use_batch) ? 10 * N : N;
	guint block_size = BLOCK_SIZES[index];
	gfloat ratio = RW_RATIOS[rw_index];
	g_autofree guint* pattern = g_malloc0(n * sizeof(guint));
	g_autofree guint* rw_pattern = g_malloc0(n * sizeof(guint));

	index = (index + 1) % NUM_SIZES;
	rw_index = (rw_index + 1) % NUM_RW_RATIOS;

	generate_rw_pattern(rw_pattern, n, ratio);
	(*generator)(pattern, n);

	_benchmark_object_read_write(run, use_batch, pattern, rw_pattern, block_size, n);
}

// WRITE

static void
benchmark_object_write_seq(BenchmarkRun* run)
{
	benchmark_object_write(run, FALSE, generate_pattern_seq);
}

static void
benchmark_object_write_batch_seq(BenchmarkRun* run)
{
	benchmark_object_write(run, TRUE, generate_pattern_seq);
}

static void
benchmark_object_write_rand(BenchmarkRun* run)
{
	benchmark_object_write(run, FALSE, generate_pattern_rand);
}

static void
benchmark_object_write_rand_batch(BenchmarkRun* run)
{
	benchmark_object_write(run, TRUE, generate_pattern_rand);
}

static void
benchmark_object_write_zipf(BenchmarkRun* run)
{
	benchmark_object_write(run, FALSE, generate_pattern_zipf);
}

static void
benchmark_object_write_zipf_batch(BenchmarkRun* run)
{
	benchmark_object_write(run, TRUE, generate_pattern_zipf);
}

// RW

static void
benchmark_object_rw_seq(BenchmarkRun* run)
{
	benchmark_object_read_write(run, FALSE, generate_pattern_seq);
}

static void
benchmark_object_rw_batch_seq(BenchmarkRun* run)
{
	benchmark_object_read_write(run, TRUE, generate_pattern_seq);
}

static void
benchmark_object_rw_rand(BenchmarkRun* run)
{
	benchmark_object_read_write(run, FALSE, generate_pattern_rand);
}

static void
benchmark_object_rw_rand_batch(BenchmarkRun* run)
{
	benchmark_object_read_write(run, TRUE, generate_pattern_rand);
}

static void
benchmark_object_rw_zipf(BenchmarkRun* run)
{
	benchmark_object_read_write(run, FALSE, generate_pattern_zipf);
}

static void
benchmark_object_rw_zipf_batch(BenchmarkRun* run)
{
	benchmark_object_read_write(run, TRUE, generate_pattern_zipf);
}

static void
add_rw_benches(const gchar* path, BenchmarkFunc benchmark)
{
	for (guint i = 0; i < NUM_SIZES; i++)
	{
		for (guint j = 0; j < NUM_RW_RATIOS; j++)
		{
			g_autofree gchar* buf = g_malloc0(64 * sizeof(gchar));
			g_snprintf(buf, 64ul, "%s <%.2fw> %dB", path, (double)RW_RATIOS[j], BLOCK_SIZES[i]);

			j_benchmark_add(buf, benchmark);
		}
	}
}

static void
add_benches(const gchar* path, BenchmarkFunc benchmark)
{
	for (guint i = 0; i < NUM_SIZES; i++)
	{
		g_autofree gchar* buf = g_malloc0(64 * sizeof(gchar));
		g_snprintf(buf, 64ul, "%s %dB", path, BLOCK_SIZES[i]);

		j_benchmark_add(buf, benchmark);
	}
}

/*
 * The mixed benchmark pre-writes the entire file to guarantee that it can be read from.
 * This has performance implications as the relevant data will be (partially) present in the page cache.
 * Some interfaces are also affected. To illustrate the problem on an example:
 * 1. mmap empty file (assume the application artificially increases the size of the file to N bytes to avoid remapping later on)
 * 2. write N+1 bytes to file
 * - 3a. without pre-writing remapping is required
 * - 3b. with pre-writing the remapping has already occured or has been mmap'ed at the appropriate size to begin with. No remapping is required.
 * 
 * Therefore, the benchmark also includes pure write operations where no pre-writing takes place.
 * This can be used to estimate the impact of pre-writing in the mixed benchmark by comparing the write benchmark with the corresponding mixed benchmark at ratio=1.00.
*/
void
benchmark_object_rw(void)
{
	// MIXED
	add_rw_benches("/object/object/rw/seq", benchmark_object_rw_seq);
	add_rw_benches("/object/object/rw/seq-batch", benchmark_object_rw_batch_seq);
	add_rw_benches("/object/object/rw/rand", benchmark_object_rw_rand);
	add_rw_benches("/object/object/rw/rand-batch", benchmark_object_rw_rand_batch);
	add_rw_benches("/object/object/rw/zipf", benchmark_object_rw_zipf);
	add_rw_benches("/object/object/rw/zipf-batch", benchmark_object_rw_zipf_batch);

	// WRITE
	add_benches("/object/object/rw/write-seq", benchmark_object_write_seq);
	add_benches("/object/object/rw/write-seq-batch", benchmark_object_write_batch_seq);
	add_benches("/object/object/rw/write-rand", benchmark_object_write_rand);
	add_benches("/object/object/rw/write-rand-batch", benchmark_object_write_rand_batch);
	add_benches("/object/object/rw/write-zipf", benchmark_object_write_zipf);
	add_benches("/object/object/rw/write-zipf-batch", benchmark_object_write_zipf_batch);
}
