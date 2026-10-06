#
# corDB - the coraine NGSI-LD broker's in-process database: its current-state plugins (corDB.so - with
# --troe corDB its history too - and ramDB.so) and ramDB's temporal plugin (troe/ramDB.so, a ring in RAM).
#
# Built against its siblings, as every Cor-Lib is (sources use -I.. and include "corTree/CorNode.h"),
# and against the broker's plugin interface - the DbDriver / TroeDriver headers - in the coraine
# checkout beside it (../coraine/src/lib). The broker resolves the NGSI-LD and Cor-Lib symbols the
# plugin calls at dlopen (it is linked -rdynamic), so the plugin links none of them.
#
#   make                 debug build (-DDEBUG -DCOR_T_ON), the same flags coraine's CMake gives a plugin
#   make BUILD=release   release build (-O2 -g, no traces)
#   make BUILD=coverage  instrumented (--coverage), as coraine's `make coverage` builds the broker
#   make install         into $(PLUGIN_DIR)/db/currentState (corDB.so, ramDB.so) and $(PLUGIN_DIR)/troe/temporal
#   make di / ci         install / clean + install
#
# The broker's feature switches the sources read - a plugin must be built as its broker was:
#   COR_FEATURE_SUBSCRIPTIONS=0|1  COR_FEATURE_REGISTRATIONS=0|1   (default 1, as coraine's)
#
# Each flavour keeps its objects AND its plugins in obj/<BUILD> - switching flavours never leaves the other
# flavour's .so in place (a debug build after a release one relinks nothing, so a shared output would be
# the release one). OBJDIR and OUT move both elsewhere - coraine's reduced-build check builds a variant
# per configuration that way, without touching this tree's own builds.
#
# Copyright 2026 Seamware
# SPDX-License-Identifier: Apache-2.0
#
CC           ?= gcc
COR_LIBS     ?= ..
CORAINE      ?= $(COR_LIBS)/coraine
PLUGIN_DIR   ?= /opt/seamware/plugins
BUILD        ?= debug
OBJDIR       ?= obj/$(BUILD)
OUT          ?= $(OBJDIR)

#
# The broker's plugin interface is what corDB is built against. Without the coraine checkout beside it
# (the corLibs umbrella builds the stack before coraine exists in a Docker build), corDB skips itself
# with the reason - coraine's own `make di` / `make i` build it. COR_DB_REQUIRED=1 makes that an error.
#
HAVE_CORAINE  = $(wildcard $(CORAINE)/src/lib/db/DbDriver.h)

COR_FEATURE_SUBSCRIPTIONS ?= 1
COR_FEATURE_REGISTRATIONS ?= 1

INCLUDE       = -I$(COR_LIBS) -I$(CORAINE)/src/lib -I$(CORAINE)/src/plugins
DEFINES       = -DLOG_ON -DCOR_FEATURE_SUBSCRIPTIONS=$(COR_FEATURE_SUBSCRIPTIONS) -DCOR_FEATURE_REGISTRATIONS=$(COR_FEATURE_REGISTRATIONS)
# The current-state plugin's version; troe/corDbRegister.c defines its own
VERSION_DEF   = -DPLUGIN_VERSION=\"0.2.0\"
CFLAGS        = -Wall -Werror -Wundef -fPIC $(INCLUDE) $(DEFINES) -MMD -MP $(EXTRA_CFLAGS)

ifeq ($(BUILD),debug)
CFLAGS       += -g -DDEBUG -DCOR_T_ON
else ifeq ($(BUILD),release)
CFLAGS       += -O2 -g
else ifeq ($(BUILD),coverage)
CFLAGS       += -g -O0 --coverage -fprofile-update=atomic -DCOR_T_ON
EXTRA_LDFLAGS += --coverage
else
$(error BUILD must be debug, release or coverage, not '$(BUILD)')
endif

LIBS          = -lgeos_c -lm

SOURCES       = corDbGlobals.c corDbIndex.c corDbStore.c corDbLog.c corDbPersist.c corDbReplay.c corDbHistory.c corDbHistoryWrite.c corDbTroe.c corDbTroeWrite.c corDbRegister.c corDbInit.c corDbClose.c \
                corDbEntityCreate.c corDbEntityBulkCreate.c corDbEntityBulkUpdate.c corDbEntityBulkMerge.c \
                corDbEntityBulkDelete.c corDbEntityRetrieve.c corDbEntityQuery.c corDbEntityDelete.c \
                corDbEntityMerge.c corDbEntityReplace.c corDbEntityAttrsSet.c corDbTypeList.c corDbAttrList.c \
                corDbGeoMatch.c corDbDoc.c corDbSysTimes.c

ifeq ($(COR_FEATURE_SUBSCRIPTIONS),1)
SOURCES      += corDbSubscriptionCreate.c corDbSubscriptionRetrieve.c corDbSubscriptionQuery.c \
                corDbSubscriptionUpdate.c corDbSubscriptionReplace.c corDbSubscriptionDelete.c
endif

ifeq ($(COR_FEATURE_REGISTRATIONS),1)
SOURCES      += corDbRegistrationCreate.c corDbRegistrationRetrieve.c corDbRegistrationQuery.c \
                corDbRegistrationUpdate.c corDbRegistrationDelete.c
endif

#
# The geo matcher is the broker's, shared with its mongoc plugin: compiled from the coraine checkout
#
GEOMATCH      = $(CORAINE)/src/plugins/shared/geoMatch.c

OBJECTS       = $(SOURCES:%.c=$(OBJDIR)/%.o) $(OBJDIR)/geoMatch.o
TROE_OBJECTS  = $(OBJDIR)/troe/ramDbRegister.o

#
# ramDB - corDB in RAM only: the same sources built with COR_DB_RAM_ONLY=1 - no disk options, no
# history (corDbTroe.c left out: no troeRegister, so the broker refuses --troe corDB with it). For a
# deployment that wants the fastest pub/sub and accepts that a restart starts empty.
#
RAM_OBJDIR    = $(OBJDIR)/ram
RAM_SOURCES   = $(filter-out corDbTroe.c corDbTroeWrite.c,$(SOURCES))
RAM_OBJECTS   = $(RAM_SOURCES:%.c=$(RAM_OBJDIR)/%.o) $(OBJDIR)/geoMatch.o

DEPS          = $(OBJECTS:.o=.d) $(TROE_OBJECTS:.o=.d) $(RAM_SOURCES:%.c=$(RAM_OBJDIR)/%.d)

PLUGIN        = $(OUT)/corDB.so
RAM_PLUGIN    = $(OUT)/ramDB.so
TROE_PLUGIN   = $(OUT)/troe/ramDB.so

ifeq ($(HAVE_CORAINE),)
ifeq ($(COR_DB_REQUIRED),1)
$(error corDB: $(CORAINE)/src/lib/db/DbDriver.h not found - the coraine checkout must be beside corDB)
endif
all install i di ci cdi debug:
	@echo "corDB: skipped - no coraine checkout at $(CORAINE) (the broker's plugin interface); coraine's make di builds it"
clean:
	rm -rf obj *~
.PHONY: all install i di ci cdi debug clean
else

all: $(PLUGIN) $(TROE_PLUGIN) $(RAM_PLUGIN)

$(RAM_PLUGIN): $(RAM_OBJECTS)
	@mkdir -p $(dir $@)
	$(CC) -shared $(RAM_OBJECTS) -o $@ $(LIBS) $(EXTRA_LDFLAGS)

$(PLUGIN): $(OBJECTS)
	@mkdir -p $(dir $@)
	$(CC) -shared $(OBJECTS) -o $@ $(LIBS) $(EXTRA_LDFLAGS)

$(TROE_PLUGIN): $(TROE_OBJECTS)
	@mkdir -p $(dir $@)
	$(CC) -shared $(TROE_OBJECTS) -o $@ $(EXTRA_LDFLAGS)

#
# The flags are a dependency: a change of BUILD, a feature switch or EXTRA_CFLAGS rebuilds everything
#
$(OBJDIR)/.flags: FORCE
	@mkdir -p $(OBJDIR)
	@echo '$(CC) $(CFLAGS)' | cmp -s - $@ || echo '$(CC) $(CFLAGS)' > $@
FORCE:

$(OBJDIR)/troe/%.o: troe/%.c $(OBJDIR)/.flags
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

$(OBJDIR)/%.o: %.c $(OBJDIR)/.flags
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(VERSION_DEF) -DCOR_DB_RAM_ONLY=0 -c $< -o $@

$(RAM_OBJDIR)/%.o: %.c $(OBJDIR)/.flags
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(VERSION_DEF) -DCOR_DB_RAM_ONLY=1 -c $< -o $@

$(OBJDIR)/geoMatch.o: $(GEOMATCH) $(OBJDIR)/.flags
	$(CC) $(CFLAGS) -c $< -o $@

install: all
	mkdir -p $(PLUGIN_DIR)/db/currentState $(PLUGIN_DIR)/troe/temporal
	cp -p $(PLUGIN)      $(PLUGIN_DIR)/db/currentState/corDB.so
	cp -p $(TROE_PLUGIN) $(PLUGIN_DIR)/troe/temporal/ramDB.so
	rm -f $(PLUGIN_DIR)/troe/temporal/corDB.so                  # the ring's old name: --troe corDB is corDB.so's own now
	cp -p $(RAM_PLUGIN)  $(PLUGIN_DIR)/db/currentState/ramDB.so

i:     install
di:    install
ci:    clean install
cdi:   clean install
debug: all

clean:
	rm -rf obj *~

-include $(DEPS)

.PHONY: all install i di ci cdi debug clean FORCE

endif
