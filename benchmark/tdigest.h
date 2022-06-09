struct TDigest;
typedef struct TDigest TDigest;

/**
 * Instantiates a new TDigest to store double values.
 * Space complexity is O(compression)
 *
 * \param[in] compression number of cells to use for approximation, 100 is a good start value. 
 */
TDigest* julea_t_digest_init(int compression);

void julea_t_digest_fini(TDigest*);

/** adds a value to the TDigest **/
void julea_t_digest_add(TDigest*,double value);

/**
 * query the q quantil(approximation), high and low quantils have a much higher precision
 * \retval NAN on error, eg no values provided so far
 */
double julea_t_digest_quantiles(TDigest*,double q);

/** query the max value (precise) */
double julea_t_digest_min(TDigest*);

/** query the min value (precise) */
double julea_t_digest_max(TDigest*);
