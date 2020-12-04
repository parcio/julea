CFLAGS := -std=c11 -Wall -Wextra -Wpedantic -Og $(shell pkg-config --cflags glib-2.0 julea julea-object julea-kv julea-db)
LDLIBS := $(shell pkg-config --libs glib-2.0 julea julea-object julea-kv julea-db)

BIN = hello-world

all: $(BIN)

clean:
	rm -f $(BIN)

run: $(BIN)
	/julea/scripts/setup.sh start
	./$(BIN)
	/julea/scripts/setup.sh stop
