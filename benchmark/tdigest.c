/**
 * T-Digest implementation based on https://github.com/tdunning/t-digest
 * appoximates quantiles with buckest to handle abetrarie sizes.
 * Buckets are filled uequaly, therefore quantiles at the extreme
 * are more precise then in the center.
 **/

#define _GNU_SOURCE

#include "tdigest.h"
#include <glib.h>
#include <stdlib.h>
#include <math.h>
#include <assert.h>

#undef G_LOG_DOMAIN
#define G_LOG_DOMAIN "TDigest"

#define USE_TOW_LEVEL_COMPRESSION 1
#define USE_WEIGHT_LIMIT 1
#ifndef NDEBUG
#define T_DIGEST_VALIDATION
#endif

/** \todo add to jhelper?
 * \param[in] data address of start of array
 * \param[in] size of one element
 * \param[in] length number of elements in array
 */
void inverse_array(void*, int, int);

void
inverse_array(void* data, int size, int length)
{
	char* f_itr;
	char* b_itr;
	char buf[8];
	if (length == 0)
	{
		return;
	}
	f_itr = data;
	b_itr = (char*)data + size * (length - 1);
	assert(size <= 8);
	while (f_itr < b_itr)
	{
		memcpy(buf, f_itr, size);
		memcpy(f_itr, b_itr, size);
		memcpy(b_itr, buf, size);
		f_itr += size;
		b_itr -= size;
	}
}

typedef struct
{
	int size;
	int capacity;
	double total_weight; /// \todo use guint64 instead of double?
	double* weight;
	double* mean;
} Cells;

struct TDigest
{
	int merge_count;
	Cells cells;
	Cells temp_cells;
	int* temp_order;
	int compression;
	double min;
	double max;
#ifdef T_DIGEST_VALIDATION
	GArray* values;
#endif
};

/**
 * Scale function K3
 * \sa https://github.com/tdunning/t-digest/blob/main/core/src/main/java/com/tdunning/math/stats/ScaleFunction.java
 *
 * \param[in] compression of t-digest to normalize
 * \param[in] totalWeight of data which sholud be distributed in t-digest
 * \return scale normalizer, used for other scale functions
 * \sa scale_next_q(), scale_max()
 */
double scale_normalizer(double, double);
/**
 * Calculates weight limit for next bucket, based on weight limit of last bucket.
 * Weight limits are represente normalized [0,1] by totalWeight
 * \param[in] q weight limit of last bucket
 * \param[in] normalizer calculaeted from scale_normalizer()
 * \return next weight limit normolized
 */
double scale_next_q(double, double);
double scale_max(double, double);

void cells_init(Cells*, int);
void cells_fini(Cells*);

/**
 * Calculates stable sorted permutation of cell array.
 *
 * \param[in] cells which temporally cells should be sorted
 * \param[out] permutation stable sorted permutation of cells
 *
 * \sa cmp_cells_indirect
 */
void cells_sort_stable(Cells*, int*);

/**
 * Compare operator to compare two cells based of there indices
 *
 * \param[in] a_id id of cell a
 * \param[in] b_id id of cell b
 * \param[in] cells Cells to compare on
 */
int cmp_cells_indirect(const void*, const void*, void*);

/** merge temporally clusters in t-digest
 */
void t_digest_merge(TDigest*);
/**
 * weighted mean of two numbers
 *
 * \param x1 first value
 * \param w1 weight of first value
 * \param x2 second value
 * \param w2 weight of second value
 */
double weighted_mean(double, double, double, double);

double
scale_normalizer(double compression, double totalWeight)
{
	return compression / (4 * log(totalWeight / compression) + 21);
}
double
scale_next_q(double x, double normalizer)
{
	double k1;
	if (x > 0.5)
	{
		x = 1. - x;
	}
	k1 = log(2 * x) * normalizer + 1.;
	if (k1 <= 0)
	{
		return exp(k1 / normalizer) / 2;
	}
	else
	{
		return 1 - exp(-k1 / normalizer) / 2;
	}
}
double
scale_max(double q, double normalizer)
{
	if (q > 0.5)
	{
		q = 1 - q;
	}
	return q / normalizer;
}

double
weighted_mean(double x1, double w1, double x2, double w2)
{
	double z;
	// element 1 is smaller then element 2
	if (x1 > x2)
	{
		double t = x1;
		x1 = x2;
		x2 = t;
		t = w1;
		w1 = w2;
		w2 = t;
	}
	z = (x1 * w1 + x2 * w2) / (w1 + w2);
	// needed to make the julea_t_digest_quantiles works
	// special case only on elements in two adjacent cells
	if (z > x1)
	{
		return x1;
	}
	if (z < x2)
	{
		return x2;
	}
	return z;
}

void
cells_init(Cells* this, int capacity)
{
	this->weight = malloc(sizeof(*this->weight) * capacity);
	this->mean = malloc(sizeof(*this->mean) * capacity);
	this->total_weight = 0;
	this->size = 0;
	this->capacity = capacity;
}
void
cells_fini(Cells* this)
{
	free(this->weight);
	free(this->mean);
}

int
cmp_cells_indirect(const void* a_id, const void* b_id, void* args)
{
	int a = *(const int*)a_id;
	int b = *(const int*)b_id;
	Cells* cells = args;
	if (cells->mean[a] < cells->mean[b])
	{
		return -1;
	}
	if (cells->mean[a] > cells->mean[b])
	{
		return 1;
	}
	if (a < b)
	{
		return -1;
	}
	if (a > b)
	{
		return 1;
	}
	return 0;
}

void
cells_sort_stable(Cells* this, int* order)
{
	for (int i = 0; i < this->size; ++i)
	{
		order[i] = i;
	}
	qsort_r(order, this->size, sizeof(int), cmp_cells_indirect, this);
}

TDigest*
julea_t_digest_init(int compression)
{
	TDigest* this = malloc(sizeof(TDigest));
	int sizeFugde = 0;
	int capacity;
	int bufferSize;
	if (compression < 10)
	{
		compression = 10;
	}
	if (USE_WEIGHT_LIMIT)
	{
		sizeFugde = compression < 30 ? 30 : 10;
	}
	if (USE_TOW_LEVEL_COMPRESSION)
	{
		/// \todo check if needed
		this->compression = compression * 2;
	}
	else
	{
		this->compression = compression;
	}
	capacity = ceil(2 * this->compression) + sizeFugde;
	/// \todo check runtime if smaller (smallest 2)
	bufferSize = 5 * capacity;
	cells_init(&this->cells, capacity);
	cells_init(&this->temp_cells, bufferSize);
	this->temp_order = malloc(sizeof(*this->temp_order) * bufferSize);

	this->min = DBL_MAX;
	this->max = DBL_MIN;

#ifdef T_DIGEST_VALIDATION
	this->values = g_array_new(FALSE, FALSE, sizeof(double));
#endif

	return this;
}

void
julea_t_digest_fini(TDigest* this)
{
	cells_fini(&this->cells);
	cells_fini(&this->temp_cells);
	free(this->temp_order);
#ifdef T_DIGEST_VALIDATION
	g_array_free(this->values, FALSE);
#endif
	free(this);
}

double
julea_t_digest_min(TDigest* this)
{
	t_digest_merge(this);
	(void)this;
	return this->min;
}

double
julea_t_digest_max(TDigest* this)
{
	t_digest_merge(this);
	return this->max;
}
double julea_t_digest_impl(TDigest*, double);
int cmp_double(const void*, const void*);
int
cmp_double(const void* x, const void* y)
{
	double a = *(const double*)x;
	double b = *(const double*)y;
	if (a < b)
	{
		return -1;
	}
	if (b < a)
	{
		return 1;
	}
	return 0;
}

double
julea_t_digest_quantiles(TDigest* this, double q)
{
	double res = julea_t_digest_impl(this, q);
#ifdef T_DIGEST_VALIDATION
	double idd;
	int lb, ub;
	double ref;
	g_array_sort(this->values, cmp_double);
	idd = q * this->values->len;
	lb = floor(idd);
	ub = ceil(idd);
	ref = lb != ub ? g_array_index(this->values, double, lb) * (idd - lb)
				 + g_array_index(this->values, double, ub) * (ub - idd)
		       : g_array_index(this->values, double, ub);
	g_debug("precision: precise: %f, t-digest: %f => %f%%", ref, res, round((res - ref) / ref * 100.));
#endif

	return res;
}
double
julea_t_digest_impl(TDigest* this, double q)
{
	double z1;
	double z2;
	double index;
	int size;
	double totalWeight;
	double* weight;
	double* mean;
	double max;
	double min;
	double weights_so_far;
	g_return_val_if_fail(q >= 0 && q <= 1, NAN);

	t_digest_merge(this);

	// special cases: no or one cluster.
	g_return_val_if_fail(this->cells.size > 0, NAN);
	if (this->cells.size == 1)
	{
		return this->cells.mean[0];
	}

	// create short codes
	size = this->cells.size;
	totalWeight = this->cells.total_weight;
	weight = this->cells.weight;
	mean = this->cells.mean;
	max = this->max;
	min = this->min;

	// calculate index of element to check
	index = q * totalWeight;
	if (index < 1)
	{
		return min;
	}
	if (index > totalWeight - 1)
	{
		return max;
	}

	weights_so_far = weight[0] / 2;
	for (int i = 0; i < size - 1; ++i)
	{
		// elements until center of next cell
		double d_next_center = (weight[i] + weight[i + 1]) / 2;
		if (weights_so_far + d_next_center > index)
		{
			double leftUnit = 0;
			double rightUnit = 0;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfloat-equal"
			if (weight[i] == 1.)
			{
				// if index equal to center of cell, return cells mean
				if (index - weights_so_far < 0.5)
				{
					return mean[i];
				}
				leftUnit = 0.5;
			}
			if (weight[i + 1] == 1.)
			{
				// if index equal to center of cell, return cells mean
				if (weights_so_far + d_next_center - index <= 0.5)
				{
					return mean[i + 1];
				}
				rightUnit = 0.5;
			}
#pragma GCC diagnostic pop
			// interpolate value between cells
			z1 = index - weights_so_far - leftUnit;
			z2 = weights_so_far + d_next_center - index - rightUnit;
			return weighted_mean(mean[i], z1, mean[i + 1], z2);
		}
		weights_so_far += d_next_center;
	}
	/// \todo should only happen if exactly one cluster exists?
	// if t-digest contains more then one element but only one cell,
	// then this cells must contain more then one element
	// also if the index is not reached between the n-2 and n-1 cell,
	// then the last cells also must contain more then one element
	g_return_val_if_fail(weight[size - 1] > 1, NAN);
	g_return_val_if_fail(index <= totalWeight, NAN);
	g_return_val_if_fail(index >= totalWeight - weight[size - 1] / 2, NAN);

	z1 = index - totalWeight - weight[size - 1] / 2;
	z2 = weight[size - 1] / 2 - z1;
	return weighted_mean(mean[size - 1], z1, max, z2);
}

void
julea_t_digest_add(TDigest* this, double value)
{
	Cells* c;
	int id;
	// merge if temp cells are full
	// temp cells need place to contain all existing cells to, for merging
	if (this->temp_cells.size >= this->temp_cells.capacity - this->cells.size)
	{
		t_digest_merge(this);
	}

	c = &this->temp_cells;
	id = c->size++;
	c->weight[id] = 1;
	c->mean[id] = value;
	c->total_weight += 1;
	if (value < this->min)
	{
		this->min = value;
	}
	if (value > this->max)
	{
		this->max = value;
	}

#ifdef T_DIGEST_VALIDATION
	g_array_append_val(this->values, value);
#endif
}

void
t_digest_merge(TDigest* this)
{
	int backwards;
	double total_weight;
	int* size_ptr;
	double* mean;
	double* weight;
	double weights_so_far;
	double normalizer;
	double w_limit;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfloat-equal"
	// no elements exist
	if (this->cells.size == 0 && this->temp_cells.size == 0.)
	{
		return;
	}
	// no new element to add
	if (this->temp_cells.total_weight == 0.)
	{
		return;
	}

	backwards = USE_TOW_LEVEL_COMPRESSION & (this->merge_count % 2 == 1);

	assert(this->cells.size <= 0 || this->cells.weight[0] == 1.);
	assert(this->cells.size <= 0 || this->cells.weight[this->cells.size - 1] == 1.);
#pragma GCC diagnostic pop

	// copy cells to temp cells
	memcpy(this->temp_cells.mean + this->temp_cells.size,
	       this->cells.mean,
	       sizeof(*this->cells.mean) * this->cells.size);
	memcpy(this->temp_cells.weight + this->temp_cells.size,
	       this->cells.weight,
	       sizeof(*this->cells.weight) * this->cells.size);
	this->temp_cells.size += this->cells.size;

	cells_sort_stable(&this->temp_cells, this->temp_order);

#ifndef T_DIGEST_VALIDATION
	// check order
	double last = DBL_MIN;
	for (int i = 0; i < this->temp_cells.size; ++i)
	{
		double n = this->temp_cells.mean[this->temp_order[i]];
		assert(last <= n);
		last = n;
	}
#endif

	if (backwards)
	{
		inverse_array(this->temp_order, sizeof(*this->temp_order), this->temp_cells.size);
	}
	this->cells.total_weight += this->temp_cells.total_weight;

	// create short codes
	total_weight = this->cells.total_weight;
	size_ptr = &this->cells.size;
	mean = this->cells.mean;
	weight = this->cells.weight;

	*size_ptr = 0;
	mean[*size_ptr] = this->temp_cells.mean[this->temp_order[0]];
	weight[*size_ptr] = this->temp_cells.weight[this->temp_order[0]];
	this->temp_cells.weight[this->temp_order[0]] = 0;
	weights_so_far = 0;

	normalizer = scale_normalizer(this->compression, total_weight);
	w_limit = total_weight * scale_next_q(0, normalizer);
	for (int i = 1; i < this->temp_cells.size; ++i)
	{
		int ix = this->temp_order[i];
		double proposed_weight = weight[*size_ptr] + this->temp_cells.weight[ix];
		double projected_weight = weights_so_far + proposed_weight;
		gboolean add_this;
		if (USE_WEIGHT_LIMIT)
		{
			double q0 = weights_so_far / total_weight;
			double q2 = (weights_so_far + proposed_weight) / total_weight;
			double a = scale_max(q0, normalizer);
			double b = scale_max(q2, normalizer);
			if (a < b)
			{
				b = a;
			}
			add_this = proposed_weight <= total_weight * b;
		}
		else
		{
			add_this = projected_weight <= w_limit;
		}

		if (i == 1 || i == this->temp_cells.size - 1)
		{
			add_this = 0;
		}

		if (add_this)
		{
			weight[*size_ptr] += this->temp_cells.weight[ix];
			mean[*size_ptr] = mean[*size_ptr] + (this->temp_cells.mean[ix] - mean[*size_ptr]) * this->temp_cells.weight[ix] / weight[*size_ptr];
			this->temp_cells.weight[ix] = 0;
		}
		else
		{
			weights_so_far += weight[*size_ptr];
			if (!USE_WEIGHT_LIMIT)
			{
				w_limit = total_weight * scale_next_q(weights_so_far / total_weight, normalizer);
			}

			++*size_ptr;
			mean[*size_ptr] = this->temp_cells.mean[ix];
			weight[*size_ptr] = this->temp_cells.weight[ix];
			this->temp_cells.weight[ix] = 0;
		}
	}
	++*size_ptr;
#ifndef NDEBUG
	{
		double sum = 0;
		for (int i = 0; i < *size_ptr; ++i)
		{
			sum += weight[i];
		}
		assert(sum - total_weight < 1.);
	}
#endif

	if (backwards)
	{
		inverse_array(mean, sizeof(*mean), *size_ptr);
		inverse_array(weight, sizeof(*weight), *size_ptr);
	}

	assert((int)weight[0] == 1);
	assert((int)weight[*size_ptr - 1] == 1);

	if (total_weight > 0)
	{
		if (mean[0] < this->min)
		{
			this->min = mean[0];
		}
		if (mean[*size_ptr - 1] > this->max)
		{
			this->max = mean[*size_ptr - 1];
		}
	}

	++this->merge_count;
	this->temp_cells.size = 0;
	this->temp_cells.total_weight = 0;
}
