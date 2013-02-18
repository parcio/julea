#include <jzfs.h>

#include <glib.h>

#include <string.h>

#include <stdio.h>
#include <stdio_ext.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <dlfcn.h>
#include <ctype.h>
#include <math.h>
#include <sys/time.h>
#include <time.h>
#include <pthread.h>
#define NUM_THREADS	1

static JZFSPool* pool = NULL;
static JZFSObjectSet* object_set = NULL;
pthread_mutex_t pool_mutex;

/* Creates and destroys "count" number of object sets in an object set array.
Time is measured for creating and destroying separately. */
void
j_zfs_test_object_set_create_destroy (JZFSPool* pool, gint count)
{
	struct timeval start_time, end_time;
	JZFSObjectSet* object_set_array[count];
	gchar s[100];
	gint i = 0;

	//Creating
	gettimeofday(&start_time, NULL);
	for(i = 0; i < count; i++)
		{
			sprintf(s, "object_set_%d", i);
			object_set_array[i] = j_zfs_object_set_create(pool, s);
		}
	gettimeofday(&end_time, NULL);
	gint64 mseconds_create = (end_time.tv_sec - start_time.tv_sec) * 1000000
				+ (end_time.tv_usec - start_time.tv_usec);
  	printf("Time spent in j_zfs_test_object_set_create_destroy creating: %" PRId64 " microseconds\n", mseconds_create);

	//Destroying
	gettimeofday(&start_time, NULL);
	for(i = 0; i < count; i++)
		{
			j_zfs_object_set_destroy(object_set_array[i]);
		}
	gettimeofday(&end_time, NULL);
	gint64 mseconds_destroy = (end_time.tv_sec - start_time.tv_sec) * 1000000
				+ (end_time.tv_usec - start_time.tv_usec);
  	printf("Time spent in j_zfs_test_object_set_create_destroy destroying: %" PRId64 " microseconds\n", mseconds_destroy);

	gint64 mseconds_total = mseconds_create + mseconds_destroy;
	printf("Created and destroyed: %d object_sets in %" PRId64 " microseconds.\n\n", count, mseconds_total);
}

/* Creates and destroys "count" number of objects in an object array.
Time is measured for creating and destroying separately. */
void
j_zfs_test_object_create_destroy(JZFSPool* pool, gint count)
{
	struct timeval start_time, end_time;
	JZFSObjectSet* object_set;
	JZFSObject* object_array[count];
	gint i = 0;

	object_set = j_zfs_object_set_create(pool, "object_set");

	//Creating
	gettimeofday(&start_time, NULL);
	for(i = 0; i < count; i++)
		{
			object_array[i] = j_zfs_object_create(object_set);
		}
	gettimeofday(&end_time, NULL);
	gint64 mseconds_create = (end_time.tv_sec - start_time.tv_sec) * 1000000
				+ (end_time.tv_usec - start_time.tv_usec);
  	printf("Time spent in j_zfs_test_object_create_destroy creating: %" PRId64 " microseconds\n", mseconds_create);

	//Destroying
	gettimeofday(&start_time, NULL);
	for(i = 0; i < count; i++)
		{
			j_zfs_object_destroy(object_array[i]);
		}
	gettimeofday(&end_time, NULL);
	gint64 mseconds_destroy = (end_time.tv_sec - start_time.tv_sec) * 1000000
				+ (end_time.tv_usec - start_time.tv_usec);

  	printf("Time spent in j_zfs_test_object_create_destroy destroying: %" PRId64 " microseconds\n", mseconds_destroy);
	gint64 mseconds_total = mseconds_create + mseconds_destroy;
	printf("Created and destroyed: %d objects in %" PRId64 " microseconds.\n\n", count, mseconds_total);

	j_zfs_object_set_destroy(object_set);
}

/* Creates and destroys object sets with a number of objects per set. */
void
j_zfs_test_objset_object_create_destroy(JZFSPool* pool, gint number_objsets, gint number_objects)
{
	struct timeval start_time, end_time;
	JZFSObjectSet* object_set;
	JZFSObject* object;
	gint i, j = 0;

	gettimeofday(&start_time, NULL);
	for(i = 0; i < number_objsets; i++)
	{
		object_set = j_zfs_object_set_create(pool, "object_set");

		for(j = 0; j < number_objects; j++)
		{
			object = j_zfs_object_create(object_set);
			j_zfs_object_destroy(object);
		}

		j_zfs_object_set_destroy(object_set);
	}
	gettimeofday(&end_time, NULL);

	printf("Created and destroyed: %d object_sets with %d objects each.\n", number_objsets, number_objects);
	gint64 mseconds = (end_time.tv_sec - start_time.tv_sec) * 1000000
				+ (end_time.tv_usec - start_time.tv_usec);
  	printf("Time spent in j_zfs_test_objset_object_create_destroy: %" PRId64 " microseconds\n\n", mseconds);
}

/* Writes to an array of the size of "size" and reads the same content.
Time is measured for writing and reading separately. */
void
j_zfs_test_object_write_read(JZFSPool* pool, gint size)
{
	struct timeval start_time, end_time;
	JZFSObjectSet* object_set;
	JZFSObject* object;
	gchar* dummy;
	gchar* dummy_cmp;
	dummy = malloc(size+1);
	dummy_cmp = malloc(size+1);
	dummy[size] = '\0';
	dummy_cmp[size] = '\0';
	gint i, k;
	object_set = j_zfs_object_set_create(pool, "object_set");
	object = j_zfs_object_create(object_set);

	for (i = 0; i < size; i++)
		{
			dummy_cmp[i] = 'z';
			dummy[i] = 'z';
		}

	//Write:
	gettimeofday(&start_time, NULL);
	j_zfs_object_write(object, dummy, size, 0);
	gettimeofday(&end_time, NULL);
	printf("Wrote %d Bytes.\n", size);
	gint64 mseconds_write = (end_time.tv_sec - start_time.tv_sec) * 1000000
				+ (end_time.tv_usec - start_time.tv_usec);
  	printf("Time spent in j_zfs_test_object_write_read writing: %" PRId64 " microseconds\n", mseconds_write);

	for (k = 0; k < size; k++)
		{
			dummy[k] = '\0';
		}

	//Read:
	gettimeofday(&start_time, NULL);
	j_zfs_object_read(object, dummy, size, 0);
	gettimeofday(&end_time, NULL);

	if (strcmp(dummy, dummy_cmp) != 0)
		{
			printf("XXX: %s\n", dummy);
		}

	printf("Read %d Bytes.\n", size);
	gint64 mseconds_read = (end_time.tv_sec - start_time.tv_sec) * 1000000
				+ (end_time.tv_usec - start_time.tv_usec);
  	printf("Time spent in j_zfs_test_object_write_read reading: %" PRId64 " microseconds\n\n", mseconds_read);
	j_zfs_object_destroy(object);
	j_zfs_object_set_destroy(object_set);
	free(dummy);
	free(dummy_cmp);

}

/* Writes to an array of the size of "size", reads and compares both strings.
This happens "count" number of times */
void
j_zfs_test_object_read_write(JZFSPool* pool, gint size, gint count)
{
	struct timeval start_time, end_time, start_read, end_read, start_write, end_write;
	JZFSObjectSet* object_set;
	JZFSObject* object;
	gint i, j, k = 0;
	gchar dummy[size];
	gchar dummy_cmp[size];
	gint64 mseconds_write = 0;
	gint64 mseconds_read = 0;
	dummy[size] = '\0';
	dummy_cmp[size] = '\0';
	object_set = j_zfs_object_set_create(pool, "object_set");
	object = j_zfs_object_create(object_set);

	for (i = 0; i < size; i++)
		{
			dummy_cmp[i] = 'z';
		}
	gint len = strlen(dummy_cmp);

	gettimeofday(&start_time, NULL);
	for(j = 0; j < count; j++)
	{
		for (i = 0; i < size; i++)
		{
			dummy[i] = 'z';
		}

		//Write:
		gettimeofday(&start_write, NULL);
		j_zfs_object_write(object, dummy, len, (j* len));
		gettimeofday(&end_write, NULL);
		mseconds_write = mseconds_write +
				((end_write.tv_sec - start_write.tv_sec) * 1000000
				+ (end_write.tv_usec - start_write.tv_usec));

		for (k = 0; k < len; k++)
		{
			dummy[k] = '\0';
		}

		//Read:
		gettimeofday(&start_read, NULL);
		j_zfs_object_read(object, dummy, len, (j * len));
		gettimeofday(&end_read, NULL);
		mseconds_read = mseconds_read +
				((end_read.tv_sec - start_read.tv_sec) * 1000000
				+ (end_read.tv_usec - start_read.tv_usec));

		if (strcmp(dummy, dummy_cmp) != 0)
		{
			printf("XXX: %s\n", dummy);
		}

	}
	gettimeofday(&end_time, NULL);

	//j_zfs_object_get_size(object);
	j_zfs_object_destroy(object);
	j_zfs_object_set_destroy(object_set);
	printf("Wrote and read %d Bytes %d times.\n", len, count);
	gint64 mseconds = (end_time.tv_sec - start_time.tv_sec) * 1000000
				+ (end_time.tv_usec - start_time.tv_usec);
	printf("Time spent reading: %" PRId64 " microseconds\n", mseconds_read);
	printf("Time spent writing: %" PRId64 " microseconds\n", mseconds_write);
  	printf("Time spent in j_zfs_test_object_read_write: %" PRId64 " microseconds\n\n", mseconds);
}

/* Opens and closes "count" number of objects in an object array.
Time is measured for opening and closing separately. */
void
j_zfs_test_object_open_close(JZFSPool* pool, gint count)
{
	struct timeval start_open, end_open, start_close, end_close;
	JZFSObjectSet* object_set;
	guint64 id_array[count];
	JZFSObject* object_array[count];
	JZFSObject* object;
	gint i;
	gint64 mseconds_open, mseconds_close, mseconds_total;
	object_set = j_zfs_object_set_create(pool, "object_set");

	for(i = 0; i < count; i++)
		{
			object = j_zfs_object_create(object_set);
			id_array[i] = j_zfs_get_object_id(object);
			j_zfs_object_close(object);
		}

	gettimeofday(&start_open, NULL);
	for(i = 0; i < count; i++)
		{
			object_array[i] = j_zfs_object_open(object_set, id_array[i]);
			//printf("object: %u opened\n", object_id);
		}
	gettimeofday(&end_open, NULL);
	mseconds_open = (end_open.tv_sec - start_open.tv_sec) * 1000000
				+ (end_open.tv_usec - start_open.tv_usec);

	gettimeofday(&start_close, NULL);
	for(i = 0; i < count; i++)
		{
			j_zfs_object_close(object_array[i]);
		}
	gettimeofday(&end_close, NULL);
	mseconds_close = (end_close.tv_sec - start_close.tv_sec) * 1000000
				+ (end_close.tv_usec - start_close.tv_usec);

	j_zfs_object_set_destroy(object_set);
	printf("Time spent in j_zfs_test_object_open_close opening %d objects: %" PRId64 " microseconds\n", count, mseconds_open);
	printf("Time spent in j_zfs_test_object_open_close closing %d objects: %" PRId64 " microseconds\n", count, mseconds_close);
	
	mseconds_total = mseconds_open + mseconds_close;
	printf("Opened and closed: %d objects in %" PRId64 " microseconds\n\n", count, mseconds_total);
}

// for Threads
void 
test_with_threads()
{
	JZFSObject* object;
	int i;
	printf("in test_with_threads\n");
	
	j_zfs_init();
	pthread_mutex_lock (&pool_mutex);
	pool = j_zfs_pool_open("jzfs");
	if (pool == NULL)
		g_error("pool could not be opened\n");
	printf("pool opened. \n");
	
	object_set = j_zfs_object_set_create(pool, "object_set");
	if (object_set == NULL)
	{
		object_set = j_zfs_object_set_open(pool, "object_set");
	}
	
	object = j_zfs_object_create(object_set);
	guint64 id = j_zfs_get_object_id(object);
	j_zfs_object_close(object);
	object = j_zfs_object_open(object_set, id);
	
	//object write x 1000
	for(i=0; i < 1000; i++){
		gchar dummy_w[] = "Hello world.";
		gint size = strlen(dummy_w);
		j_zfs_object_write(object, dummy_w, size, 0);
	}
	j_zfs_object_close(object);

	//Case 1: open, read x 1000, close
	object = j_zfs_object_open(object_set, id);
	for(i=0; i < 1000; i++){
		guint64 length = strlen("Hello world.");
		gchar dummy_r[length];
		j_zfs_object_read(object, dummy_r, length, 0);
	}
	j_zfs_object_close(object);

	//Case 2: (open, read, close) x 1000
	for (i=0; i < 311; i++){ // fehleranfällig bei i > 311, s. out.txt
		printf("i: %i\n", i);
		object = j_zfs_object_open(object_set, id); 
		guint64 length = strlen("Hello world.");
		gchar dummy_r[length];
		j_zfs_object_read(object, dummy_r, length, 0);
		j_zfs_object_close(object);
	}

	//j_zfs_object_destroy(object);
	j_zfs_object_set_destroy(object_set);
	j_zfs_pool_close(pool);
	printf("Pool closed.\n");
	pthread_mutex_unlock (&pool_mutex);
	j_zfs_fini();

}

gint
main (gint argc, gchar **argv)
{
	/*Time variables*/
	gchar buffer[30];
  	struct timeval start_time, end_time;
	//hrtime_t start, end; //for nanoseconds
	time_t curtime;

	//JZFS variables
	//JZFSPool* pool;
	//JZFSObjectSet* object_set;
	//JZFSObjectSet* object_set2;
	JZFSObject* object1;
	//JZFSObject* object2;
	//void* buf;
	pthread_t threads[NUM_THREADS];
	int rc;
	long t;
	void* status;

	//j_zfs_init();

	printf("\n***\n");

	/*Calculate start_time */
	gettimeofday(&start_time, NULL);
  	curtime=start_time.tv_sec;
  	strftime(buffer,30,"%T.",localtime(&curtime));
  	printf("Start: %s%ld\n",buffer,start_time.tv_usec);
	//start = gethrtime(); //for nanoseconds

	/*Open the pool*/
	//pool = j_zfs_pool_open("jzfs");

	/*gint i, j, k = 0;
	for(i = 0; i < 10; i++)
	{
		object_set = j_zfs_object_set_create(pool, "object_set");

		object_set2 = j_zfs_object_set_open(pool, "object_set");
		j_zfs_object_set_close(object_set2);

		for(j = 0; j < 10; j++)
		{
			gchar dummy[] = "Hello world.";
			gint len = strlen(dummy);

			object1 = j_zfs_object_create(object_set);
			j_zfs_object_write(object1, dummy, len, 0);

			for (k = 0; k < len; k++)
			{
				dummy[k] = '\0';
			}

			j_zfs_object_read(object1, dummy, len, 0);
			j_zfs_object_get_size(object1);

			if (strcmp(dummy, "Hello world.") != 0)
			{
				printf("XXX: %s\n", dummy);
			}

			j_zfs_object_destroy(object1);
		}

		j_zfs_object_set_destroy(object_set);
	}
	printf("Created and destroyed: %d object_sets with %d objects each.\n", i,j);*/


	/**************/
	/* Benchmarks */
	/**************/
	//j_zfs_test_object_set_create_destroy(pool, 100); //(pool, number of object sets)
	//j_zfs_test_objset_object_create_destroy(pool, 10, 1000); //(pool, number of object sets, number of objects
	//j_zfs_test_object_create_destroy(pool, 1000);//(pool, number of objects)
	//j_zfs_test_object_open_close(pool, 1000); //(pool, number of objects)
	//j_zfs_test_object_read_write(pool, 3000000, 10); //(pool, array size, how many times)
	//j_zfs_test_object_write_read(pool, 9000000); //(pool, array size)
	

	
	//object_set = j_zfs_object_set_create(pool, "object_set");
	//j_zfs_object_set_close(object_set);
	//printf("set_open\n");
	//object_set = j_zfs_object_set_open(pool, "object_set");
	//printf("obj_create\n");
	//object1 = j_zfs_object_create(object_set);
	//j_zfs_object_get_size(object1);
	//j_zfs_object_set_size(object1, 15); //object, offset
	//j_zfs_object_get_size(object1);
	//j_zfs_object_set_size(object1, 5);
	//j_zfs_object_get_size(object1);
	//j_zfs_object_destroy(object1);
	//j_zfs_object_set_close(object_set);
	//j_zfs_object_set_destroy(object_set);

	pthread_mutex_init(&pool_mutex, NULL);
	// create threads
	for(t=0; t < NUM_THREADS; t++)
	{
		rc = pthread_create(&threads[t], NULL, test_with_threads, NULL);
		if(rc)
			printf("Error creating threads\n");
	} 
	printf("Threads created.\n");
	for(t=0; t < NUM_THREADS; t++)
	{
		rc = pthread_join(threads[t], &status);
		if(rc)
		printf("Error joining.\n");
	}
	printf("Threads joined.\n");

	/*Close the pool*/
	//j_zfs_pool_close(pool);

	/*Calculate end_time */
	gettimeofday(&end_time, NULL);
  	curtime=end_time.tv_sec;
	strftime(buffer,30,"%T.",localtime(&curtime));
  	printf("\nEnd: %s%ld\n",buffer,end_time.tv_usec);

	//Microseconds:
	gint64 mseconds = (end_time.tv_sec - start_time.tv_sec) * 1000000
				+ (end_time.tv_usec - start_time.tv_usec);
  	printf("\nTime passed: %" PRId64 " microseconds\n", mseconds);
	//for nanoseconds:
	//end = gethrtime();
	//printf("\nTime passed: %lld ns\n", (end-start));


	printf("***\n\n");

	//j_zfs_fini();
	pthread_exit(NULL); //exit threads
	return 0;
}
