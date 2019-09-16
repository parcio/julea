#CFLAGS += -fsanitize=address
#LDFLAGS += -fsanitize=address

CFLAGS += -ggdb -Wall -Wextra

BIN = hello-world db-example-with-error-handling db-example

all: $(BIN)

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

clean:
	rm -f $(BIN)

run: $(BIN)
	../scripts/setup.sh start
	./$(BIN)
	../scripts/setup.sh stop
