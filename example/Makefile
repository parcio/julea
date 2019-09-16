#CFLAGS += -fsanitize=address
#LDFLAGS += -fsanitize=address

CFLAGS += -ggdb -Wall -Wextra

BIN = hello-world hdf5-example benchmark-mpi db-example-with-error-handling db-example

all: $(BIN)

benchmark-mpi:
CC=mpicc
CFLAGS += $(shell pkg-config --cflags julea-db)
LDLIBS += $(shell pkg-config --libs julea-db)
CFLAGS += $(shell pkg-config --cflags glib-2.0)
LDLIBS += $(shell pkg-config --libs glib-2.0)

db-example-with-error-handling:
CFLAGS += $(shell pkg-config --cflags julea-db)
LDLIBS += $(shell pkg-config --libs julea-db)

db-example:
CFLAGS += $(shell pkg-config --cflags julea-db)
LDLIBS += $(shell pkg-config --libs julea-db)

hello-world:
CFLAGS += $(shell pkg-config --cflags julea-object)
LDLIBS += $(shell pkg-config --libs julea-object)
CFLAGS += $(shell pkg-config --cflags julea-kv)
LDLIBS += $(shell pkg-config --libs julea-kv)

hdf5-example:
LDLIBS += -lhdf5

clean:
	rm -f $(BIN)

run: $(BIN)
	../scripts/setup.sh start
	./$(BIN)
	../scripts/setup.sh stop
