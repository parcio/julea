#include <glib.h>

struct JZFS;
struct JZFSPool;
struct JZFSObjectSet;
struct JZFSObject;

typedef struct JZFS JZFS;
typedef struct JZFSPool JZFSPool;
typedef struct JZFSObjectSet JZFSObjectSet;
typedef struct JZFSObject JZFSObject;

void j_zfs_init (void);
void j_zfs_fini (void);

JZFSPool* j_zfs_pool_open (gchar*);
void j_zfs_pool_close (JZFSPool*);

JZFSObjectSet* j_zfs_object_set_create (JZFSPool*, gchar*);
JZFSObjectSet* j_zfs_object_set_open (JZFSPool*, gchar*);
void j_zfs_object_set_close (JZFSObjectSet*);
void j_zfs_object_set_destroy (JZFSObjectSet*);

JZFSObject* j_zfs_object_create (JZFSObjectSet*);
JZFSObject* j_zfs_object_open (JZFSObjectSet*, guint64);
guint64 j_zfs_object_get_size (JZFSObject*);
void j_zfs_object_set_size (JZFSObject*, guint64);
void j_zfs_object_read (JZFSObject*, void*, guint64, guint64);
void j_zfs_object_write (JZFSObject*, void*, guint64, guint64);
void j_zfs_object_close (JZFSObject*);
void j_zfs_object_destroy (JZFSObject*);
