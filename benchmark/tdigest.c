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

#define USE_TOW_LEVEL_COMPRESSION 1
#define USE_WEIGHT_LIMIT 1

typedef struct {
	int size;
	int capacity;
	double totalWeight;
	double* weight;
	double* mean;
} Cells;

struct TDigest {
	int merge_count;
	Cells cells;
	Cells tempCells;
	int* tempOrder;
	double comrpession;
	double min;
	double max;
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
double scale_normalizer(double,double);
/**
 * Calculates weight limit for next bucket, based on weight limit of last bucket.
 * Weight limits are represente normalized [0,1] by totalWeight
 * \param[in] q weight limit of last bucket
 * \param[in] normalizer calculaeted from scale_normalizer()
 * \return next weight limit normolized
 */
double scale_next_q(double,double);
double scale_max(double,double);

void cells_init(Cells*,int);
void cells_fini(Cells*);
int cmp_cells_indirect(const void*,const void*,void*);
void cells_sort_stable(Cells*,int*);

void t_digest_merge(TDigest*);
/**
 * weighted mean of two numbers
 *
 * \param x1 first value
 * \param w1 weight of first value
 * \param x2 second value
 * \param w2 weight of second value
 */
double weighted_mean(double,double,double,double);


double scale_normalizer(double comrpession, double totalWeight) {
	return comrpession / (4 * log(totalWeight / comrpession) + 21);
}
double scale_next_q(double x, double normalizer) {
	if(x > 0.5) { x = 1. - x; }
	double k1 = log(2*x) * normalizer + 1.;
	if(k1 <= 0) {
		return exp(k1/normalizer) / 2;
	} else {
		return 1 - exp(-k1/normalizer) / 2;
	}
}
double scale_max(double q, double normalizer) {
	if(q > 0.5) { q = 1 - q; }
	return q / normalizer;
}

double weighted_mean(double x1, double w1, double x2, double w2) {
	return (x1 * w1 + x2 * w2) / (w1 + w2);
}
