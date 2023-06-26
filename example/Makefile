EXEC = pub sub

CFLAGS = -I../pgqmini -I/usr/include/postgresql

LDFLAGS = -L../pgqmini -lpgqmini -lpq

all: $(EXEC)

%: %.c ../pgqmini/libpgqmini.a
	gcc $(CFLAGS) $< -o $@ $(LDFLAGS)

clean:
	rm -f $(EXEC)