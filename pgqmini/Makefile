SRCS = pgqmini.c

OBJS = $(SRCS:.c=.o)

CFLAGS = -I./pgqmini -I/usr/include/postgresql

LDFLAGS = -lpq

LIB = libpgqmini.a

all: $(LIB)

$(LIB): $(OBJS)
	ar rcs $@ $^

%.o: %.c
	gcc $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(LIB)