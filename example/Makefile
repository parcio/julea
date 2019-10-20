CFLAGS  += -fsanitize=address
CFLAGS  += $(shell pkg-config --cflags glib-2.0 julea-object julea-kv)
LDFLAGS += -fsanitize=address
LDLIBS  += $(shell pkg-config --libs glib-2.0 julea-object julea-kv)

BIN = hello-world

all: $(BIN)

clean:
	rm -f $(BIN)

run: $(BIN)
	../scripts/setup.sh start
	./$(BIN)
	../scripts/setup.sh stop
