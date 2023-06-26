LIB = pgqmini/libpgqmini.a

EXAMPLE = example-exec

all: $(LIB) $(EXAMPLE)

$(LIB):
	make -C pgqmini

$(EXAMPLE):
	make -C example

install:
	cp $(LIB) /usr/local/lib/
	cp pgqmini/*.h /usr/local/include/

clean:
	make -C pgqmini clean
	make -C example clean