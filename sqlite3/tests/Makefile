V =
ifeq ($(strip $(V)),)
	E = @echo
	Q = @
else
	E = @\#
	Q =
endif
export E Q

PROGRAM = sqlite3-tests

CFLAGS = -g -Wall -std=c17 -MMD -MP

LIBS ?= -lsqlite3

OBJS += main.o
OBJS += test-close.o
OBJS += test-open.o
OBJS += test-prepare.o

all: $(PROGRAM)

%.o: %.c
	$(E) "  CC      " $@
	$(Q) $(CC) $(CFLAGS) -c $< -o $@

$(PROGRAM): $(OBJS)
	$(E) "  LINK    " $@
	$(Q) $(CC) $(LIBS) -o $@ $^

clean:
	$(E) "  CLEAN"
	$(Q) rm -f $(PROGRAM)
	$(Q) rm -f $(OBJS) *.d
.PHONY: clean

-include $(OBJS:.o=.d)
