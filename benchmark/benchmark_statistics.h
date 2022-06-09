#include <glib.h>

#include "benchmark.h"

void* j_benchmark_statistics_init(guint compression);
void  j_benchmark_statistics_fini(void*);

/**
 * Adds a new round time for to statistics.
 * round_time[0] = current_time[0]
 * round_time[n] = current_time[n] - current_time[n-1]
 *
 * \attention for long runtime looses precision due to subtraction of large floating point numbers
 * \todo check if this is a problem
 *
 * \param[inout] this
 * \param[in] current_time elapsed time spending toing benchmark.
 */
void j_benchmark_statistics_add(void*,double current_time);

double j_benchmark_statistics_min(void*);
double j_benchmark_statistics_mean(void*);
double j_benchmark_statistics_max(void*);
guint j_benchmark_statistics_num_entries(void *);
double j_benchmark_statistics_derivation(void*);
double j_benchmark_statistics_quantiles(void*,double q);

