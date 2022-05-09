#include <bits/types/struct_timeval.h>
#include <core/jmanagedbackends.h>
#include <core/jbackend.h>
#include <core/jlist.h>
#include <core/jlist-iterator.h>
#include <core/jsemantics.h>
#include <core/jtrace.h>
#include <core/jconfiguration.h>

#include <julea-config.h>

#include <glib.h>

#include <string.h>
#include <sys/select.h>
#include <sys/time.h>

#define EXE(cmd, ...) \
	do \
	{ \
		if ((cmd) == FALSE) \
		{ \
			g_warning("EXE failed at %s:%d with:", __FILE__, __LINE__); \
			g_warning(__VA_ARGS__); \
			goto end; \
		} \
	} while (FALSE)

/// an read write spin lock to avoid data access on migration
typedef struct
{
	gint read_access_count; ///< number of read access
	guint generation; ///< generation count, incremented after each write_access
	gint write_access; ///< true if a write operation is scheduled or running
} RWSpinLock;

/// acquired a read lock
/** \public \memberof RWSpinLock
 * \return data gegenartion when clock is acquired
 * \sa read_unlock */
guint read_lock(RWSpinLock* this);

/// unlock a read lock
/** \public \memberof \RWSpinLock
 * \sa read_lock */
void read_unlock(RWSpinLock* this);

/// acquire a write lock
/** \public \memberof RWSpinLock
 * this will increase the generation count
 * \return generation before
 * \sa write_unlock*/
guint write_lock(RWSpinLock* this);

/// unlock a write lock
/* \public \memberof \RWSpinLock
 * \sa write_lock */
void write_unlock(RWSpinLock* this);

#define MAX_LOG_LENGTH 8192
struct JManagedBackends
{
	struct JBackendWrapper** object_backend;
	JStorageTier** object_tier_data;
	guint32 object_backend_length;

	JSemantics* kv_semantics;
	JBackend* kv_store;
	GModule* kv_module;

	JObjectBackendPolicy* policy;
	GModule* module;

	GArray* rw_spin_locks;
	guint array_length; /// used as atomic to ensure unique ids for each object

	RWSpinLock global_lock;

	struct
	{
		const gchar* filename;
		int length;
		struct
		{
			guint64 time;
			char access;
			guint obj_id;
			guint tier;
		} entries[MAX_LOG_LENGTH];
	} log;
};

struct JManagedBackendScope
{
	void* mem;
	JManagedBackends* stack;
	guint lock_id;
	guint tier; /// \TODO include in StorageTier -> less redundance
	const gchar* namespace;
	const gchar* path;
	/// unsigned client_id; \TODO reenable?
};

struct JBackendWrapper
{
	struct JBackend backend;
	struct JBackend* orig;
	struct JStorageTier* tier_data;
	struct JManagedBackendScope scope;
};

struct KVEntry
{
	guint generation;
	guint lock_id;
	guint backend_id;
};

/// puts KVEntry in kv store.
/** \private \memberof JManagedBackends */
gboolean kv_put(JManagedBackends* this, const gchar* namespace, const gchar* key,
		struct KVEntry* entry);

/// get KVEntry in kv store.
/** \private \memberof JManagedBackends */
gboolean kv_get(JManagedBackends* this, const gchar* namespace, const gchar* key,
		struct KVEntry** entry);

/// rm KV entry in kv store.
/** \private \memberof JManagedBackends */
gboolean kv_rm(JManagedBackends* this, const gchar* namespace, const gchar* key);

#define ACCESS(TYPE, DATA, LETTER) \
	if (this->scope.stack->log.filename) \
	{ \
		int i = g_atomic_int_add(&this->scope.stack->log.length, 1); \
		if (i < MAX_LOG_LENGTH) \
		{ \
			this->scope.stack->log.entries[i].time = g_get_monotonic_time(); \
			this->scope.stack->log.entries[i].access = LETTER; \
			this->scope.stack->log.entries[i].obj_id = this->scope.lock_id; \
			this->scope.stack->log.entries[i].tier = this->scope.tier; \
		} \
	} \
	this->scope.stack->policy->process_access( \
		this->scope.stack->policy->data, \
		this->scope.namespace, \
		this->scope.path, \
		this->scope.lock_id, \
		this->scope.tier, \
		J_OBJECT_ACCESS_##TYPE, \
		DATA)

static gboolean
backend_status(JBackend* this_raw, gpointer object, gint64* modification_time, guint64* size)
{
	J_TRACE_FUNCTION(NULL);
	struct JBackendWrapper* this = (struct JBackendWrapper*)this_raw;
	gboolean ret;
	ACCESS(STATUS, NULL, 'o');

	ret = this->orig->object.backend_status(
		this->orig,
		object,
		modification_time,
		size);

	return ret;
}
static gboolean
backend_sync(JBackend* this_raw, gpointer object)
{
	J_TRACE_FUNCTION(NULL);
	struct JBackendWrapper* this = (struct JBackendWrapper*)this_raw;
	gboolean ret;

	ACCESS(SYNC, NULL, 'o');

	ret = this->orig->object.backend_sync(this->orig, object);

	return ret;
}

static gboolean
backend_read(JBackend* this_raw, gpointer object, gpointer buffer, guint64 length, guint64 offset, guint64* bytes_read)
{
	J_TRACE_FUNCTION(NULL);
	struct JBackendWrapper* this = (struct JBackendWrapper*)this_raw;
	gboolean ret;
	struct JObjectBackendRWAccess data = {
		.length = length,
		.offset = offset,
	};

	ACCESS(READ, &data, 'r');

	ret = this->orig->object.backend_read(this->orig, object, buffer, length, offset, bytes_read);

	return ret;
}

static gboolean
backend_write(JBackend* this_raw, gpointer object, gconstpointer buffer, guint64 length, guint64 offset, guint64* bytes_written)
{
	J_TRACE_FUNCTION(NULL);
	struct JBackendWrapper* this = (struct JBackendWrapper*)this_raw;
	gboolean ret;
	struct JObjectBackendRWAccess data = {
		.length = length,
		.offset = offset
	};

	ACCESS(WRITE, &data, 'w');

	ret = this->orig->object.backend_write(this->orig, object, buffer, length, offset, bytes_written);

	return ret;
}

static gboolean
backend_delete(JBackend* this_raw, gpointer object)
{
	J_TRACE_FUNCTION(NULL);
	struct JBackendWrapper* this = (struct JBackendWrapper*)this_raw;
	gboolean ret;

	ACCESS(DELETE, NULL, 'd');

	/// \todo reuse lock
	kv_rm(this->scope.stack, this->scope.namespace, this->scope.path);
	ret = this->orig->object.backend_delete(this->orig, object);

	return ret;
}

guint
read_lock(RWSpinLock* this)
{
	while (true)
	{
		while (this->write_access)
			;
		g_atomic_int_inc(&this->read_access_count);
		if (this->write_access)
		{
			g_atomic_int_dec_and_test(&this->read_access_count);
		}
		else
		{
			return this->generation;
		}
	}
}

void
read_unlock(RWSpinLock* this)
{
	g_atomic_int_dec_and_test(&this->read_access_count);
}

guint
write_lock(RWSpinLock* this)
{
	while (!g_atomic_int_compare_and_exchange(&this->write_access, 0, 1))
		;
	while (this->read_access_count)
		;
	return this->generation++;
}

void
write_unlock(RWSpinLock* this)
{
	g_atomic_int_set(&this->write_access, 0);
}

/// proxy function
gchar const* const* j_configuration_get_object_tiers(JConfiguration* config);
gchar const* const*
j_configuration_get_object_tiers(JConfiguration* config)
{
	(void)config;
	return NULL;
}

gboolean
j_backend_managed_init(JConfiguration* config, JList* object_backends, JManagedBackends** instance_ptr)
{
	JListIterator* itr;
	gchar const* const* tier_itr;
	JManagedBackends* this;
	struct JBackendWrapper** b_itr;
	struct JStorageTier** t_itr;
	const gchar* policy_name = j_configuration_get_object_policy(config);
	char const* const* policy_args = j_configuration_get_object_policy_args(config);
	JObjectBackendPolicy* (*module_backend_policy_info)(void) = NULL;
	JObjectBackendPolicy* tmp_policy;
	*instance_ptr = malloc(sizeof(JManagedBackends));
	this = *instance_ptr;

	// init spin lock array
	this->rw_spin_locks = g_array_sized_new(false, false, sizeof(RWSpinLock), 100);
	this->array_length = 0;

	// setup backends
	this->object_backend_length = j_list_length(object_backends);
	this->object_backend = malloc(sizeof(struct JBackendWrapper*) * this->object_backend_length);
	this->object_tier_data = malloc(sizeof(JStorageTier*) * this->object_backend_length);
	itr = j_list_iterator_new(object_backends);
	tier_itr = j_configuration_get_object_tiers(config);
	b_itr = this->object_backend;
	t_itr = this->object_tier_data;
	while (j_list_iterator_next(itr))
	{
		*b_itr = malloc(sizeof(struct JBackendWrapper));
		memcpy(*b_itr, j_list_iterator_get(itr), sizeof(JBackend));
		(*b_itr)->backend.object.backend_write = backend_write;
		(*b_itr)->backend.object.backend_read = backend_read;
		(*b_itr)->backend.object.backend_status = backend_status;
		(*b_itr)->backend.object.backend_sync = backend_sync;
		(*b_itr)->backend.object.backend_delete = backend_delete;
		(*b_itr)->orig = j_list_iterator_get(itr);
		(*b_itr)->scope = (struct JManagedBackendScope){ 0 };
		(*b_itr)->tier_data = malloc(sizeof(JStorageTier));
		if (tier_itr && *tier_itr)
			memcpy((*b_itr)->tier_data, tier_itr++, sizeof(JStorageTier));
		else
			memset((*b_itr)->tier_data, 0, sizeof(JStorageTier));

		*t_itr = (*b_itr)->tier_data;
		++t_itr;
		++b_itr;
	}
	g_assert_true(tier_itr == FALSE || *tier_itr == FALSE);
	j_list_iterator_free(itr);

	// load policy
	this->module = NULL;
	{
		const gchar* module_name = g_strdup_printf("policy-object-%s", policy_name);
		gchar* path = g_module_build_path(JULEA_BACKEND_PATH, module_name);
		this->module = g_module_open(path, G_MODULE_BIND_LOCAL);
		if (this->module == NULL)
		{
			g_warning("Could not load policy module: %s.", path);
			goto end;
		}
		g_free(path);
	}
	g_module_symbol(this->module, "backend_policy_info", (gpointer*)&module_backend_policy_info);
	if (module_backend_policy_info == NULL)
	{
		g_warning("unable to find entry point in backend policy module!");
		goto end;
	}
	tmp_policy = module_backend_policy_info();
	if (tmp_policy == NULL)
	{
		g_warning("failed to get policy info!");
		goto end;
	}
	if (tmp_policy->process_create == NULL
	    || tmp_policy->process_message == NULL
	    || tmp_policy->process_access == NULL
	    || tmp_policy->init == NULL
	    || tmp_policy->process == NULL)
	{
		g_warning("Policy don't provide all functions necessary");
		goto end;
	}
	this->policy = malloc(sizeof(JObjectBackendPolicy));
	memcpy(this->policy, tmp_policy, sizeof(JObjectBackendPolicy));

	/// \todo use helper list
	this->policy->init(&this->policy->data, policy_args, this);

	// setup kv
	EXE(j_backend_load_server(j_configuration_get_object_policy_kv_backend(config),
				  "server",
				  J_BACKEND_TYPE_KV,
				  &this->kv_module,
				  &this->kv_store),
	    "failed to create kv for backend manager!");
	EXE(this->kv_store->kv.backend_init(j_configuration_get_object_policy_kv_path(config),
					    &this->kv_store->data),
	    "failed to init kv for backend manager!");
	this->kv_semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);

	// this->log.filename = j_configuration_get_object_policy_log_file(config);
	this->log.length = 0;

	return TRUE;
end:
	return FALSE;
}

gboolean
j_backend_managed_fini(JManagedBackends* this)
{
	if (this->log.filename)
	{
		if (this->log.length > MAX_LOG_LENGTH)
		{
			g_warning("more memory access registred then log size: %d access, but only %d spaces", this->log.length, MAX_LOG_LENGTH);
		}
		FILE* fp = fopen(this->log.filename, "w");
		fprintf(fp, "time,obj,type,tier\n");
		for (int i = 0; i < this->log.length; ++i)
		{
			fprintf(fp, "%lu,%u,%c,%u\n",
				this->log.entries[i].time,
				this->log.entries[i].obj_id,
				this->log.entries[i].access,
				this->log.entries[i].tier);
		}
		fclose(fp);
		g_message("object stack access log stored in %s", this->log.filename);
	}

	this->policy->fini(this->policy->data);
	free(this->policy);
	free(this->object_backend);
	g_array_unref(this->rw_spin_locks);
	g_module_close(this->module);
	free(this);

	return TRUE;
}

/// \todo doku
gboolean j_backend_managed_object_create(JManagedBackends* this, const gchar* namespace, const gchar* path, JBackend** backend, JManagedBackendScope** scope);

/// \TODO optimize for only one backend
/// \TODO remove scope return <- just complete virtual
gboolean
j_backend_managed_object_open(JManagedBackends* this,
			      const gchar* namespace,
			      const gchar* path,
			      JBackend** backend,
			      JManagedBackendScope** scope)
{
	struct KVEntry* entry;
	struct JBackendWrapper* wrapper;
	gboolean ret = FALSE;

	if (kv_get(this, namespace, path, &entry))
	{
		guint generation;

		generation = read_lock(&g_array_index(this->rw_spin_locks, RWSpinLock, entry->lock_id));
		if (generation != entry->generation)
		{
			EXE(kv_get(this, namespace, path, &entry), "failed to fetch kvEntry for open");
		}

		*backend = g_memdup2(this->object_backend[entry->backend_id], sizeof(struct JBackendWrapper));
		wrapper = (void*)*backend;
		wrapper->scope = (JManagedBackendScope){
			.mem = wrapper,
			.lock_id = entry->lock_id,
			.tier = entry->backend_id,
			.stack = this,
			.namespace = namespace,
			.path = path,
		};
		*scope = &wrapper->scope;
	}
	else
	{
		EXE(j_backend_managed_object_create(this, namespace, path, backend, scope),
		    "failed to create new entry");
	}
	ret = TRUE;
end:
	return ret;
}

gboolean
j_backend_managed_object_create(JManagedBackends* this,
				const gchar* namespace,
				const gchar* path,
				JBackend** backend,
				JManagedBackendScope** scope)
{
	gboolean ret = FALSE;
	struct JBackendWrapper* wrapper;
	struct KVEntry entry;
	guint lock_id;
	guint tier;
	RWSpinLock newLock;

	// allocate new spinlock with read lock
	newLock = (RWSpinLock){ .generation = 0, .read_access_count = 1, .write_access = 0 };
	g_array_append_val(this->rw_spin_locks, newLock);
	lock_id = g_atomic_int_add(&this->array_length, 1);

	// ask policy for tier
	EXE(this->policy->process_create(this->policy->data, namespace, path, lock_id, &tier),
	    "failed to match storage tier for new object!");
	*backend = g_memdup2(this->object_backend[tier], sizeof(struct JBackendWrapper));
	wrapper = (void*)*backend;
	if (this->log.filename)
	{
		guint64 i = g_atomic_int_add(&this->log.length, 1);
		if (i < MAX_LOG_LENGTH)
		{
			this->log.entries[i].access = 'c';
			this->log.entries[i].obj_id = lock_id;
			this->log.entries[i].time = g_get_monotonic_time();
			this->log.entries[i].tier = tier;
		}
	}

	entry = (struct KVEntry){
		.lock_id = lock_id,
		.backend_id = tier,
		.generation = 0
	};

	EXE(kv_put(this, namespace, path, &entry),
	    "failed to store new object in kv");

	wrapper->scope = (JManagedBackendScope){
		.mem = wrapper,
		.lock_id = entry.lock_id,
		.stack = this,
		.tier = entry.backend_id,
		.namespace = namespace,
		.path = path
	};
	*scope = &wrapper->scope;

	ret = TRUE;
end:
	return ret;
}

gboolean
j_backend_managed_object_close(JManagedBackendScope* this)
{
	read_unlock(&g_array_index(this->stack->rw_spin_locks, RWSpinLock, this->lock_id));
	free(this->mem);
	return TRUE;
}

gboolean
j_backend_managed_policy_message(JManagedBackends* this,
				 const gchar* type, gpointer data, guint length)
{
	if (this->policy->process_message)
	{
		return this->policy->process_message(this->policy->data, type, data, length);
	}
	return TRUE;
}

gboolean
j_backend_managed_get_tiers(JManagedBackends* this, JStorageTier const* const** tiers, guint* length)
{
	if (tiers)
	{
		*tiers = (JStorageTier const* const*)this->object_tier_data;
	}
	*length = this->object_backend_length;
	return TRUE;
}

guint
j_backend_managed_get_tier(JManagedBackends* this,
			   const gchar* namespace, const gchar* path)
{
	struct KVEntry* entry;
	EXE(kv_get(this, namespace, path, &entry),
	    "failed to fetch kventry");
	return entry->backend_id;
end:
	return -1;
}

gboolean
kv_put(JManagedBackends* this, const gchar* namespace, const gchar* key,
       struct KVEntry* entry)
{
	gpointer batch;
	EXE(j_backend_kv_batch_start(this->kv_store, namespace, this->kv_semantics, &batch),
	    "failed do start kv batch");
	EXE(j_backend_kv_put(this->kv_store, batch, key, entry, sizeof(struct KVEntry)),
	    "failed batch put value command");
	EXE(j_backend_kv_batch_execute(this->kv_store, batch),
	    "failed to execute put value batch");
	return TRUE;
end:
	return FALSE;
}

gboolean
kv_get(JManagedBackends* this, const gchar* namespace, const gchar* key,
       struct KVEntry** entry)
{
	gpointer batch;
	guint32 len;
	gboolean ret;
	EXE(j_backend_kv_batch_start(this->kv_store, namespace, this->kv_semantics, &batch),
	    "failed do start kv batch");
	ret = j_backend_kv_get(this->kv_store, batch, key, (gpointer*)entry, &len);
	EXE(j_backend_kv_batch_execute(this->kv_store, batch),
	    "failed to execute get value batch");
#ifndef NDEBUG
	if (len != sizeof(struct KVEntry))
	{
		ret = FALSE;
	}
#endif
	return ret;
end:
	return FALSE;
}

gboolean
kv_rm(JManagedBackends* this, const gchar* namespace, const gchar* key)
{
	gpointer batch;
	gboolean ret = FALSE;

	EXE(j_backend_kv_batch_start(this->kv_store, namespace, this->kv_semantics, &batch),
	    "failed to start kv backend");
	ret = j_backend_kv_delete(this->kv_store, batch, key);
	EXE(j_backend_kv_batch_execute(this->kv_store, batch),
	    "failed to execute delete kv value");
end:
	return ret;
}

gboolean
j_backend_managed_lock(JManagedBackends* this,
		       JBackend*** backends,
		       guint* length)
{
	write_lock(&this->global_lock);
	if (backends != NULL)
	{
		*backends = (void*)this->object_backend;
		*length = this->object_backend_length;
	}
	return TRUE;
}

/// returns management to stack
gboolean
j_backend_managed_unlock(JManagedBackends* this)
{
	(void)this;
	write_unlock(&this->global_lock);
	return TRUE;
}

gboolean
j_backend_managed_get_all(JManagedBackends* this, gchar const* namespace, gpointer* iterator)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(this != NULL, FALSE);

	return j_backend_kv_get_all(this->kv_store, namespace, iterator);
}

gboolean
j_backend_managed_get_by_prefix(JManagedBackends* this, gchar const* namespace, gchar const* prefix, gpointer* iterator)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(this != NULL, FALSE);

	return j_backend_kv_get_by_prefix(this->kv_store, namespace, prefix, iterator);
}

gboolean
j_backend_managed_iterate(JManagedBackends* this, gpointer iterator, gchar const** name)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(this != NULL, FALSE);

	guint32 length;
	gconstpointer data;

	return j_backend_kv_iterate(this->kv_store, iterator, name, &data, &length);
}

guint64
j_backend_storage_tier_get_bandwidth(const JStorageTier* this)
{
	return this->bandwidth;
}

guint64
j_backend_storage_tier_get_latency(const JStorageTier* this)
{
	return this->latency;
}

guint64
j_backend_storage_tier_get_capacity(const JStorageTier* this)
{
	return this->capacity;
}

gboolean
j_backend_managed_object_migrate(JManagedBackends* this,
				 const gchar* namespace,
				 const gchar* path,
				 guint dest)
{
	struct KVEntry* entry;
	struct KVEntry new_entry;
	gpointer object_from, object_to;
	gboolean ret = FALSE;
	gboolean lock = FALSE;
	JBackend* from;
	JBackend* to = this->object_backend[dest]->orig;
	gpointer data = NULL;

	read_lock(&this->global_lock);
	EXE(kv_get(this, namespace, path, &entry), "Unable to migrate, because entry not found!");
	if (entry->backend_id == dest)
	{
		ret = TRUE;
		goto end;
	}
	guint generation = write_lock(&g_array_index(this->rw_spin_locks, RWSpinLock, entry->lock_id));
	lock = TRUE;
	if (generation != entry->generation)
	{
		EXE(kv_get(this, namespace, path, &entry), "failed open kvEntry again for migration");
	}
	from = this->object_backend[entry->backend_id]->orig;
	if (this->log.filename)
	{
		int i = g_atomic_int_add(&this->log.length, 1);
		if (i < MAX_LOG_LENGTH)
		{
			this->log.entries[i].access = 'm';
			this->log.entries[i].time = g_get_monotonic_time();
			this->log.entries[i].obj_id = entry->lock_id;
			this->log.entries[i].tier = dest;
		}
	}

	{
		const guint64 max_chunk_size = 1024 * 1024 * 64; ///< 64MB max chunk size for transfer \todo make configurable
		guint64 size;
		gint64 mod_time;
		guint64 offset = 0;
		if (!j_backend_object_open(from, namespace, path, &object_from))
		{
			/// \todo remove data (object was deleted!)
			ret = TRUE;
			goto end;
		}
		EXE(j_backend_object_status(from, object_from, &mod_time, &size),
		    "Failed to get object size");
		data = malloc(MIN(size, max_chunk_size));

		EXE(j_backend_object_create(to, namespace, path, &object_to),
		    "Failed to create new object");
		while (size > 0)
		{
			guint64 written, transfer = MIN(size, max_chunk_size);
			EXE(j_backend_object_read(from, object_from, data, transfer, offset, &written) && transfer == written,
			    "Failed to read object for transmission");
			EXE(j_backend_object_write(to, object_to, data, transfer, offset, &written) && written == transfer,
			    "Failed to write migrated object");
			size -= written;
			offset += written;
		}

		EXE(j_backend_object_delete(from, object_from),
		    "Failed to delete original object");
		EXE(j_backend_object_close(to, object_to),
		    "Failed to close new object");
	}

	new_entry = *entry;
	++new_entry.generation;
	new_entry.backend_id = dest;
	EXE(kv_put(this, namespace, path, &new_entry), "failed to store new migrated entry");
	ret = TRUE;
end:

	if (data != NULL)
		free(data);
	if (lock)
	{
		write_unlock(&g_array_index(this->rw_spin_locks, RWSpinLock, entry->lock_id));
	}
	read_unlock(&this->global_lock);
	return ret;
}

gboolean
j_backend_managed_policy_process(JManagedBackends* this, gboolean* keep_running)
{
	gboolean ret = TRUE;
	g_message("start process");
	do
	{
		ret &= this->policy->process(this->policy->data);
	} while (ret && *keep_running);
	return ret;
}
