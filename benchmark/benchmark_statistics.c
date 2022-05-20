#include "benchmark_statistics.h"
#include "tdigest.h"

#include <string.h>
#include <math.h>

typedef struct {
	TDigest* t_digest;
	double sum;
	double sq_sum;
	guint cnt;
	double last_time;	
} StatisticalData;

void*
j_benchmark_statistics_init(guint compression)
{
	StatisticalData* this = malloc(sizeof(StatisticalData));

	memset(this, 0, sizeof(*this));
	this->t_digest = t_digest_init(compression);

	return this;
}

void
j_benchmark_statistics_fini(void* raw)
{
	StatisticalData* this = raw;
	g_return_if_fail(this != NULL);

	t_digest_fini(this->t_digest);
	free(this);
}

void
j_benchmark_statistics_add(void* raw, double current_time)
{
	StatisticalData* this = raw;
	g_return_if_fail(this != NULL);

	double round_time = this->last_time - current_time;
	this->sum += round_time;
	this->sq_sum += round_time*round_time;
	++this->cnt;
	t_digest_add(this->t_digest, round_time);
	this->last_time = current_time;
}

double
j_benchmark_statistics_quantiles(void* raw, double q)
{
	StatisticalData* this = raw;
	g_return_val_if_fail(raw != NULL, NAN);

	return t_digest_quantiles(this->t_digest, q);
}

double
j_benchmark_statistics_min(void* raw)
{
	StatisticalData* this = raw;
	g_return_val_if_fail(raw != NULL, NAN);

	return t_digest_min(this->t_digest);
}

double
j_benchmark_statistics_max(void* raw)
{
	StatisticalData* this = raw;
	g_return_val_if_fail(raw != NULL, NAN);

	return t_digest_max(this->t_digest);
}

double
j_benchmark_statistics_mean(void* raw)
{
	StatisticalData* this = raw;
	g_return_val_if_fail(raw != NULL, NAN);

	return this->sum / this->cnt;
}

double
j_benchmark_statistics_derivation(void* raw)
{
	StatisticalData* this = raw;
	g_return_val_if_fail(raw != NULL, NAN);

	return sqrt(this->cnt*this->sq_sum - this->sum*this->sum) / this->cnt;
}

