CFLAGS ?= -O2 -g
CFLAGS += -std=c11 -Wall -Wextra -Werror -D_POSIX_C_SOURCE=200809L

.PHONY: all clean

all: readdir_hash

readdir_hash: readdir_hash.c
	$(CC) $(CFLAGS) $< -o $@

clean:
	$(RM) readdir_hash
