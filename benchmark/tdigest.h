struct TDigest;
typedef struct TDigest TDigest;
TDigest* t_digest_init(double comrpession);
void t_digest_fini(TDigest*);
void t_digest_add(TDigest*,double value);
double t_digest_quantiles(TDigest*,double q);
double t_digest_min(TDigest*);
double t_digest_max(TDigest*);
