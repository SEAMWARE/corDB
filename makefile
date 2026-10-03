#
# corDB - the coraine NGSI-LD broker's in-process database: its current-state plugin (corDB.so) and its
# temporal plugin (troe/corDB.so).
#
# Built against its siblings, as every Cor-Lib is (sources use -I.. and include "corTree/CorNode.h"),
# and against the broker's plugin interface - the DbDriver / TroeDriver headers - in the coraine
# checkout beside it (../coraine/src/lib). The broker resolves the NGSI-LD and Cor-Lib symbols the
# plugin calls at dlopen (it is linked -rdynamic), so the plugin links none of them.
#
#   make                 debug build (-DDEBUG -DCOR_T_ON), the same flags coraine's CMake gives a plugin
#   make BUILD=release   release build (-O2 -g, no traces)
#   make install         into $(PLUGIN_DIR)/db/currentState and $(PLUGIN_DIR)/troe/temporal
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
else
$(error BUILD must be debug or release, not '$(BUILD)')
endif

LIBS          = -lgeos_c -lm

SOURCES       = corDbGlobals.c corDbIndex.c corDbStore.c corDbRegister.c corDbInit.c corDbClose.c \
                corDbEntityCreate.c corDbEntityBulkCreate.c corDbEntityBulkUpdate.c corDbEntityBulkMerge.c \
                corDbEntityBulkDelete.c corDbEntityRetrieve.c corDbEntityQuery.c corDbEntityDelete.c \
                corDbEntityMerge.c corDbEntityReplace.c corDbEntityAttrsSet.c corDbTypeList.c corDbAttrList.c \
                corDbGeoMatch.c

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
TROE_OBJECTS  = $(OBJDIR)/troe/corDbRegister.o
DEPS          = $(OBJECTS:.o=.d) $(TROE_OBJECTS:.o=.d)

PLUGIN        = $(OUT)/corDB.so
TROE_PLUGIN   = $(OUT)/troe/corDB.so

all: $(PLUGIN) $(TROE_PLUGIN)

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
	$(CC) $(CFLAGS) $(VERSION_DEF) -c $< -o $@

$(OBJDIR)/geoMatch.o: $(GEOMATCH) $(OBJDIR)/.flags
	$(CC) $(CFLAGS) -c $< -o $@

install: all
	mkdir -p $(PLUGIN_DIR)/db/currentState $(PLUGIN_DIR)/troe/temporal
	cp -p $(PLUGIN)      $(PLUGIN_DIR)/db/currentState/corDB.so
	cp -p $(TROE_PLUGIN) $(PLUGIN_DIR)/troe/temporal/corDB.so

i:   install
di:  install
ci:  clean install

clean:
	rm -rf obj *~

-include $(DEPS)

.PHONY: all install i di ci clean FORCE
