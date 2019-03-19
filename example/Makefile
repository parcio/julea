CFLAGS  += -fsanitize=address $(shell pkg-config --cflags julea-object)
LDFLAGS += -fsanitize=address $(shell pkg-config --libs julea-object)

BIN = hello-world

all: $(BIN)

clean:
	rm -f $(BIN)

run: $(BIN)
	../scripts/setup.sh start
	./$(BIN)
	../scripts/setup.sh stop
