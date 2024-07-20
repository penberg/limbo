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
LIBS += -lm

OBJS += main.o
OBJS += test-aux.o
OBJS += test-close.o
OBJS += test-open.o
OBJS += test-prepare.o

all: $(PROGRAM)

test: $(PROGRAM)
	$(E) "  TEST"
	$(Q) $(CURDIR)/$(PROGRAM)

%.o: %.c
	$(E) "  CC      " $@
	$(Q) $(CC) $(CFLAGS) -c $< -o $@

$(PROGRAM): $(OBJS)
	$(E) "  LINK    " $@
	$(Q) $(CC) -o $@ $^ $(LIBS)

clean:
	$(E) "  CLEAN"
	$(Q) rm -f $(PROGRAM)
	$(Q) rm -f $(OBJS) *.d
.PHONY: clean

-include $(OBJS:.o=.d)
