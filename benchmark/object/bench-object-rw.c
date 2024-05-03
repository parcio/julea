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

#define min(x, y) (((x) <= (y)) ? (x) : (y))

static const guint BLOCK_SIZE = 4 * 1024;

static const guint NUM_SIZES = 3;

static guint index = 0;

static const guint BLOCK_SIZES[5] = { 1024, 2 * 1024, 4 * 1024 };

static const guint N = 5;

static void
_benchmark_object_read(BenchmarkRun* run, gboolean use_batch, guint* pattern, guint block_size, guint n)
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
			j_object_read(object, dummy, block_size, pattern[i] * block_size, &nb, batch);

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

/*
Fills a buffer of a given length with the values in the range [0, len) shuffled using a set seed.
*/
static void
generate_pattern_rand(guint* buf, guint len)
{
	// insert sequence
	generate_pattern_seq(buf, len);

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

	g_autofree guint* template = g_malloc0(len * sizeof(guint));
	generate_pattern_seq(template, len);

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
}

static void
benchmark_object_read(BenchmarkRun* run, gboolean use_batch, void (*generator)(guint*, guint))
{
	guint n = (use_batch) ? 10 * N : N;
	guint block_size = BLOCK_SIZES[index];
	g_autofree guint* pattern = g_malloc0(n * sizeof(guint));

	printf("block size: %d\n", block_size);
	index = (index + 1) % NUM_SIZES;

	(*generator)(pattern, n);

	/*
	printf("======= Pattern =======\n");
	for (guint i = 0; i < n; i++)
	{
		printf("%d ", pattern[i]);
	}
	printf("\n");
	*/

	_benchmark_object_read(run, use_batch, pattern, BLOCK_SIZE, n);
}

static void
benchmark_object_read_seq(BenchmarkRun* run)
{
	benchmark_object_read(run, FALSE, generate_pattern_seq);
}

static void
benchmark_object_read_batch_seq(BenchmarkRun* run)
{
	benchmark_object_read(run, TRUE, generate_pattern_seq);
}

static void
benchmark_object_read_rand(BenchmarkRun* run)
{
	benchmark_object_read(run, FALSE, generate_pattern_rand);
}

static void
benchmark_object_read_rand_batch(BenchmarkRun* run)
{
	benchmark_object_read(run, TRUE, generate_pattern_rand);
}

static void
benchmark_object_read_zipf(BenchmarkRun* run)
{
	benchmark_object_read(run, FALSE, generate_pattern_zipf);
}

static void
benchmark_object_read_zipf_batch(BenchmarkRun* run)
{
	benchmark_object_read(run, TRUE, generate_pattern_zipf);
}

static void
add_benches(const gchar* path, BenchmarkFunc benchmark)
{
	for (guint i = 0; i < NUM_SIZES; i++)
	{
		g_autofree gchar* buf = g_malloc0(64 * sizeof(gchar));
		g_snprintf(buf, 64ul, "%s > %dB", path, BLOCK_SIZES[i]);
		j_benchmark_add(buf, benchmark);
	}
}

void
benchmark_object_rw(void)
{
	add_benches("/object/object/read-seq", benchmark_object_read_seq);
	add_benches("/object/object/rw/read-seq-batch", benchmark_object_read_batch_seq);
	add_benches("/object/object/rw/read-rand", benchmark_object_read_rand);
	add_benches("/object/object/rw/read-rand-batch", benchmark_object_read_rand_batch);
	add_benches("/object/object/rw/read-zipf", benchmark_object_read_zipf);
	add_benches("/object/object/rw/read-zipf-batch", benchmark_object_read_zipf_batch);
}
