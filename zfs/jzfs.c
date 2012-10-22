#include <jzfs.h>

#include <glib.h>

#include <sys/zfs_znode.h>
#include <sys/spa.h>
#include <sys/dmu.h>
#include <sys/txg.h>
#include <sys/dbuf.h>
#include <sys/dmu_objset.h>
#include <sys/poll.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/zio.h>
#include <sys/zil.h>
#include <sys/zil_impl.h>
#include <sys/vdev_impl.h>
#include <sys/vdev_file.h>
#include <sys/spa_impl.h>
#include <sys/metaslab_impl.h>
#include <sys/dsl_prop.h>
#include <sys/dsl_dataset.h>
#include <sys/dsl_scan.h>
#include <sys/zio_checksum.h>
#include <sys/refcount.h>
#include <stdio.h>
#include <stdio_ext.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <dlfcn.h>
#include <ctype.h>
#include <math.h>
#include <sys/fs/zfs.h>
#include <libnvpair.h>
#include <sys/time.h>
#include <time.h>

static guint64
ztest_tx_assign(dmu_tx_t *tx, guint64 txg_how, const gchar *tag)
{
	guint64 txg;
	gint error;
	//printf("in dmu_tx_assign\n");
	/*
	 * Attempt to assign tx to some transaction group.
	 */
	error = dmu_tx_assign(tx, txg_how);
	if (error) {
		if (error == ERESTART) {
			ASSERT(txg_how == TXG_NOWAIT);
			dmu_tx_wait(tx);
		} else {
//			ASSERT3U(error, ==, ENOSPC);
//			ztest_record_enospc(tag);
		}
		dmu_tx_abort(tx);
		return (0);
	}
	txg = dmu_tx_get_txg(tx);
	ASSERT(txg != 0);
	return (txg);
}

struct JZFS
{
	gint ref_count;
};

struct JZFSPool
{
	JZFS* zfs;
	gchar* name;
	spa_t* spa;

	gint ref_count;
};

struct JZFSObjectSet
{
	JZFSPool* pool;
	gchar* name;
	objset_t* object_set;

	gint ref_count;
};

struct JZFSObjectHeader
{
	guint64 object_size;
};

typedef struct JZFSObjectHeader JZFSObjectHeader;

struct JZFSObject
{
	JZFSObjectSet* object_set;
	guint64 object;
	JZFSObjectHeader object_header;

	gint ref_count;
};

static JZFS* jzfs = NULL;

static
JZFS*
j_zfs_ref (JZFS* zfs)
{
	g_atomic_int_inc(&(zfs->ref_count));

	return zfs;
}

static
void
j_zfs_unref (JZFS* zfs)
{
	if (g_atomic_int_dec_and_test(&(zfs->ref_count)))
	{
		g_slice_free(JZFS, zfs);
	}
}

void
j_zfs_init (void)
{
	assert(jzfs == NULL);

	jzfs = g_slice_new(JZFS);
	jzfs->ref_count = 1;

	kernel_init(FREAD | FWRITE);
}

void
j_zfs_fini (void)
{
	assert(jzfs != NULL);

	kernel_fini();

	j_zfs_unref(jzfs);

	jzfs = NULL;
}

static
JZFSPool*
j_zfs_pool_new (JZFS* zfs, gchar* name)
{
	JZFSPool* pool;

	pool = g_slice_new(JZFSPool);
	pool->zfs = j_zfs_ref(zfs);
	pool->name = g_strdup(name);
	pool->ref_count = 1;

	return pool;
}

static
JZFSPool*
j_zfs_pool_ref (JZFSPool* pool)
{
	g_atomic_int_inc(&(pool->ref_count));

	return pool;
}

static
void
j_zfs_pool_unref (JZFSPool* pool)
{
	if (g_atomic_int_dec_and_test(&(pool->ref_count)))
	{
		j_zfs_unref(pool->zfs);

		g_free(pool->name);
		g_slice_free(JZFSPool, pool);
	}
}

/*
JZFSPool*
j_zfs_pool_create(JZFS* jzfs, gchar* name)
{
	JZFSPool* pool;

	printf("\nCreate pool %s.\n", name);
	nvlist_t *nvroot;
	gint ret;

	pool = j_zfs_pool_new(jzfs, name);

	//(void) spa_destroy(name);
	shared->zs_vdev_next_leaf = 0;
	nvroot = make_vdev_root("/dev/sdb", NULL, 0, 0,
	    0, 0, 0, 1);

	ret = spa_create(name, nvroot, NULL, NULL, NULL);
	assert(ret == 0);
	nvlist_free(nvroot);

	printf("Pool %s created. pool_create() works.\n", name);

	ret = spa_open(name, &(pool->spa), jzfs);
	assert(ret == 0);

	return pool;
}
*/

/*Opens a storage pool. */
JZFSPool*
j_zfs_pool_open(gchar* name)
{
	JZFSPool* pool;
	gint ret;

	if(jzfs == NULL)
		return 0;

	pool = j_zfs_pool_new(jzfs, name);
	if(spa_open(name, &(pool->spa), jzfs) != 0)
		return 0;

	printf("Pool opened.\n\n");

	return pool;
}

/*Closes a storage pool. */
void
j_zfs_pool_close(JZFSPool* pool)
{
	//printf("Minref: %i \n", pool->spa->spa_minref);
	printf("\nClosing %s...\n", spa_name(pool->spa));
	printf("refcount: %p %" PRId64 "\n", &pool->spa->spa_refcount,
		refcount_count(&pool->spa->spa_refcount));

	spa_close(pool->spa, pool->zfs);

	printf("Pool closed.\n");

	j_zfs_pool_unref(pool);
}

/*
void
j_zfs_pool_destroy(JZFSPool* pool)
{
	gint ret;

	ret = spa_destroy(pool->name);
	assert(ret == 0);
	printf("\nPool %s destroyed. pool_destroy() works.\n", pool->name);
}
*/

static
JZFSObjectSet*
j_zfs_object_set_new (JZFSPool* pool, gchar* name)
{
	JZFSObjectSet* object_set;

	object_set = g_slice_new(JZFSObjectSet);
	object_set->pool = j_zfs_pool_ref(pool);
	object_set->name = g_strdup(name);
	object_set->ref_count = 1;

	return object_set;
}

static
JZFSObjectSet*
j_zfs_object_set_ref (JZFSObjectSet* object_set)
{
	g_atomic_int_inc(&(object_set->ref_count));

	return object_set;
}

static
void
j_zfs_object_set_unref (JZFSObjectSet* object_set)
{
	if (g_atomic_int_dec_and_test(&(object_set->ref_count)))
	{
		j_zfs_pool_unref(object_set->pool);

		g_free(object_set->name);
		g_slice_free(JZFSObjectSet, object_set);
	}
}

/* Creates an object set */
JZFSObjectSet*
j_zfs_object_set_create(JZFSPool* pool, gchar* name)
{
	JZFSObjectSet* object_set;

	gchar* fullname;
	gint ret;

	object_set = j_zfs_object_set_new(pool, name);

	fullname = g_strdup_printf("%s/%s", pool->spa->spa_name, name);

	if(dmu_objset_create(fullname, DMU_OST_OTHER, 0, NULL, NULL) != 0)
		return 0;

	dmu_objset_hold(fullname, object_set, &object_set->object_set);

	/*guint64 id = dmu_objset_id(object_set->object_set);
	printf("object_set %s ID %" PRId64 " created.\n", fullname, id);*/

	g_free(fullname);

	return object_set;
}

/* Opens an object set */
JZFSObjectSet*
j_zfs_object_set_open(JZFSPool* pool, gchar* name)
{
	JZFSObjectSet* object_set;

	gchar* fullname;
	gint ret;

	object_set = j_zfs_object_set_new(pool, name);

	fullname = g_strdup_printf("%s/%s", pool->spa->spa_name, name);

	if(dmu_objset_own(fullname, DMU_OST_OTHER, B_FALSE, object_set, &object_set->object_set) != 0)
		return 0;

	guint64 id2 = dmu_objset_id(object_set->object_set);

	printf("object_set %s ID %" PRId64 " opened.\n", fullname, id2);

	g_free(fullname);

	return object_set;
}

/* Closes an object set */
void
j_zfs_object_set_close(JZFSObjectSet* object_set)
{

	guint64 os_id= dmu_objset_id(object_set->object_set);
	dmu_objset_disown(object_set->object_set, object_set);

	printf("object_set %s/%s ID %" PRId64 " closed.\n", object_set->pool->spa->spa_name,
		object_set->name, os_id);

	j_zfs_object_set_unref(object_set);
}

/* Destroys an object set */
void
j_zfs_object_set_destroy(JZFSObjectSet* object_set)
{
	gint ret;
	gchar* fullname;
	//guint64 os_id= dmu_objset_id(object_set->object_set);

	dmu_objset_rele(object_set->object_set, object_set);
	fullname = g_strdup_printf("%s/%s", object_set->pool->spa->spa_name, object_set->name);

	ret = dmu_objset_destroy(fullname, B_FALSE);
	assert (ret==0);
	//printf("object_set %s ID %" PRId64 " destroyed.\n", fullname, os_id);

	g_free(fullname);

	j_zfs_object_set_unref(object_set);
}

static
JZFSObject*
j_zfs_object_new (JZFSObjectSet* object_set)
{
	JZFSObject* object;

	object = g_slice_new(JZFSObject);
	object->object_set = j_zfs_object_set_ref(object_set);
	object->object = 0;
	object->ref_count = 1;
	object->object_header.object_size = 0;

	return object;
}

static
JZFSObject*
j_zfs_object_ref (JZFSObject* object)
{
	g_atomic_int_inc(&(object->ref_count));

	return object;
}

static
void
j_zfs_object_unref (JZFSObject* object)
{
	if (g_atomic_int_dec_and_test(&(object->ref_count)))
	{
		j_zfs_object_set_unref(object->object_set);

		g_slice_free(JZFSObject, object);
	}
}

/* Reads from an object.
   Flags = DMU_READ_PREFETCH, DMU_READ_NO_PREFETCH
*/
static void
j_zfs_object_read_internal(JZFSObject* object, void* buf, guint64 length, guint64 offset)
{
	gint ret;

	ret = dmu_read(object->object_set->object_set, object->object, offset, length, buf, DMU_READ_NO_PREFETCH);
	assert(ret == 0);
	//printf("Read %" PRId64 " Bytes from Object %" PRId64 " at offset %" PRId64 "\n",
	//		length, object->object, offset);
}

/* Writes to an object. */
static void
j_zfs_object_write_internal(JZFSObject* object, void* buf, guint64 length, guint64 offset)
{
	dmu_tx_t *tx;
	dmu_buf_t *db;
	guint64 txg;

	VERIFY3U(0, ==, dmu_bonus_hold(object->object_set->object_set, object->object, FTAG, &db));
	tx = dmu_tx_create(object->object_set->object_set);

	if(length >= 2000000) {
		gint i = 0;
		gint j = length;

		while(j >= 2000000) {
			dmu_tx_hold_write(tx, object->object, offset + i, 2000000);
			i += 2000000;
			j = length -i;
		}
	}

	txg = ztest_tx_assign(tx, TXG_WAIT, FTAG);
	if (txg == 0) {
		printf("in j_zfs_object_write_internal: txg == 0 !\n");
		return;
	}

	//size > 2MB
	if(length >= 2000000){
		gint i = 0;
		gint j = length;

		while(j >= 2000000) {
			dmu_write(object->object_set->object_set, object->object, offset + i, 2000000, buf + i, tx);
			i += 2000000;
			j = length - i;
		}
		if(j>0)
			dmu_write(object->object_set->object_set, object->object, offset + i, j, buf + i, tx);
	}
	else
		dmu_write(object->object_set->object_set, object->object, offset, length, buf, tx);
	dmu_tx_commit(tx);
	//printf("Wrote %" PRId64 " Bytes to Object %" PRId64 " at offset %" PRId64 "\n", length, object->object, offset);


	dmu_buf_rele(db, FTAG);
}

guint64
j_zfs_get_object_id(JZFSObject * object)
{
	return object->object;
}

/*Creates an object */
JZFSObject*
j_zfs_object_create(JZFSObjectSet* object_set)
{
	gint ret;
	JZFSObject* object;
	guint64 os_id;

	dmu_buf_t *db;
	dmu_tx_t *tx;
	guint64 txg;
	gint error = 0;

	object = j_zfs_object_new(object_set);

	os_id= dmu_objset_id(object_set->object_set);

	tx = dmu_tx_create(object_set->object_set);

	dmu_tx_hold_bonus(tx, DMU_NEW_OBJECT);

	txg = ztest_tx_assign(tx, TXG_WAIT, object_set->name);

	if (txg == 0) {
		// FIXME
		ret = ENOSPC;
		return 0;
	}

	if(dmu_objset_zil(object_set->object_set)->zl_replay != !!object->object)
		return 0;

	object->object = dmu_object_alloc(object_set->object_set,
	    DMU_OT_UINT64_OTHER, 0, DMU_OT_UINT64_OTHER,
	    dmu_bonus_max(), tx);

	if (error) {
		ASSERT3U(error, ==, EEXIST);
		dmu_tx_commit(tx);
		// FIXME
		ret = error;
		return 0;
	}

	if(object->object == 0)
		return 0;

	if(dmu_object_set_blocksize(object_set->object_set, object->object, 4096, 0, tx) != 0)
		return 0;
		
	if(dmu_bonus_hold(object_set->object_set, object->object, object_set->name, &db) != 0)
		return 0;

	dmu_buf_will_dirty(db, tx);
	dmu_buf_rele(db, object_set->name);

	dmu_tx_commit(tx);

	//printf("object %" PRId64 " of object_set %" PRId64 " created. \n",
	//	object->object, os_id);

	return object;
}

/*Opens an object */
JZFSObject*
j_zfs_object_open(JZFSObjectSet* object_set, guint64 id)
{
	JZFSObject* object;
	guint64 os_id;
	gint ret;
	dmu_buf_t *db;
	dmu_tx_t *tx;
	guint64 txg;

	object = j_zfs_object_new(object_set);

	tx = dmu_tx_create(object_set->object_set);
	dmu_tx_hold_bonus(tx, DMU_NEW_OBJECT);
	txg = ztest_tx_assign(tx, TXG_WAIT, object_set->name);
	if (txg == 0)
		return 0;

	if(dmu_object_reclaim(object_set->object_set, id, DMU_OT_UINT64_OTHER, 0, DMU_OT_UINT64_OTHER, dmu_bonus_max()) != 0)
		return 0;

	object->object = id;
	if(object->object == 0)
		return 0;

	if(dmu_object_set_blocksize(object_set->object_set, object->object, 4096, 0, tx) != 0)
		return 0;

	if(dmu_bonus_hold(object_set->object_set, object->object, object_set->name, &db) != 0)
		return 0;

	dmu_buf_rele(db, object_set->name);

	os_id= dmu_objset_id(object_set->object_set);
	//printf("object %"PRId64" of object_set %"PRId64" opened.\n", object->object, os_id);
	j_zfs_object_read_internal(object, &object->object_header, sizeof(JZFSObjectHeader), 0);
	
	dmu_tx_commit(tx);

	return object;
}

/* Gets the size of an object */
guint64
j_zfs_object_get_size(JZFSObject* object)
{
	guint64 size;
	size = object->object_header.object_size;
	printf("size of object %" PRId64 ": %" PRId64" Byte\n", object->object, size);
	return size;
}

/* Sets the size of an object (truncate only, from offset until the end of the object)*/
void
j_zfs_object_set_size(JZFSObject* object, guint64 offset)
{
	lr_truncate_t *lr;
	dmu_tx_t *tx;
	guint64 txg;
	gint error = 0;
	guint64 size = object->object_header.object_size;

	tx = dmu_tx_create(object->object_set->object_set);
	dmu_tx_hold_free(tx, object->object, offset /* FIXME offset */, size);

	txg = ztest_tx_assign(tx, TXG_WAIT, FTAG);
	if (txg == 0) {
		//ztest_range_unlock(rl);
		//ztest_object_unlock(zd, object->object);
		error = 1;
	}
	assert(error==0);

	VERIFY(dmu_free_range(object->object_set->object_set, object->object, offset /* FIXME offset */, size, tx) == 0);
	object->object_header.object_size = offset; //object->object_header.object_size = offset + size;

	printf("Size of object %"PRId64" set. New size: %" PRId64 "\n", object->object, object->object_header.object_size);

	dmu_tx_commit(tx);
}



void
j_zfs_object_read(JZFSObject* object, void* buf, guint64 length, guint64 offset)
{
	j_zfs_object_read_internal(object, buf, length, offset + sizeof(JZFSObjectHeader));
}



void
j_zfs_object_write(JZFSObject* object, void* buf, guint64 length, guint64 offset)
{
	j_zfs_object_write_internal(object, buf, length, offset + sizeof(JZFSObjectHeader));

	if(length + offset > object->object_header.object_size)
		object->object_header.object_size = offset + length;
}

/*Closes an object */
void
j_zfs_object_close(JZFSObject* object)
{
	gint ret;
	dmu_tx_t *tx;
	guint64 txg;
	guint64 os_id;

	j_zfs_object_write_internal(object, &object->object_header, sizeof(JZFSObjectHeader), 0);
	tx = dmu_tx_create(object->object_set->object_set);

	dmu_tx_hold_free(tx, object->object, 0, DMU_OBJECT_END);

	txg = ztest_tx_assign(tx, TXG_WAIT, object->object_set->name);
	if (txg == 0) {
		ret = ENOSPC;
	}
	//ret = dmu_object_free(object->object_set->object_set, object->object, tx);
	ret = dmu_object_set_blocksize(object->object_set->object_set, object->object,
					0, DN_MIN_INDBLKSHIFT ,tx);
	assert(ret == 0);
	dmu_tx_commit(tx);
	ASSERT(object->object != 0);

	os_id= dmu_objset_id(object->object_set->object_set);
	//printf("object %"PRId64" of object_set %"PRId64" closed.\n", object->object, os_id);

	j_zfs_object_unref(object);
}

/* Destroys an object. */
void
j_zfs_object_destroy(JZFSObject* object)
{
	gint ret;
	dmu_tx_t *tx;
	guint64 txg;
	//guint64 os_id= dmu_objset_id(object->object_set->object_set);
	//printf("object_set: %u \n", os_id);

	tx = dmu_tx_create(object->object_set->object_set);
	dmu_tx_hold_free(tx, object->object, 0, DMU_OBJECT_END);
	
	txg = ztest_tx_assign(tx, TXG_WAIT, FTAG);
	if (txg == 0) {
		// FIXME
		ret = ENOSPC;
	}
	
	ret = dmu_object_free(object->object_set->object_set, object->object, tx);
	assert(ret == 0);

	dmu_tx_commit(tx);


	ASSERT(object->object != 0);

	//printf("object %" PRId64 " of object_set %" PRId64 " destroyed.\n",
	//	object->object, os_id);

	if (ret != 0) {
		printf("Error\n");
		assert(ret == ENOSPC);
	} else {
			object->object = 0;
		}

	j_zfs_object_unref(object);
}
