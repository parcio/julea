#include <mpi.h>
#include <glib.h>
#include <stdio.h>

#define BENCHMARK_MPI_C

static
int world_size;
static
int world_rank;

#include "benchmark-mpi-main.c"

static
void
work(int argc, char** argv)
{
	gint i;
	for (i = 0; i < argc; i++)
	{
		printf("argv[%d] -> %s\n", i, argv[i]);
	}
	benchmark_db();
}

int
main(int argc, char** argv)
{
	// Initialize the MPI environment
	MPI_Init(&argc, &argv);

	// Get the number of processes
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);

	// Get the rank of the process
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

	// Get the name of the processor
	char processor_name[MPI_MAX_PROCESSOR_NAME];
	int name_len;
	MPI_Get_processor_name(processor_name, &name_len);

	// Print off a hello world message
	printf("Hello world from processor %s, rank %d out of %d processors\n",
		processor_name, world_rank, world_size);

	work(argc, argv);

	// Finalize the MPI environment.
	MPI_Finalize();
}
