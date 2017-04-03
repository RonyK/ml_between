ifeq ($(SCIDB),)
  X := $(shell which scidb 2>/dev/null)
  ifneq ($(X),)
    X := $(shell dirname ${X})
    SCIDB := $(shell dirname ${X})
  endif
  $(info SciDB installed at $(SCIDB))
endif

# A development environment will have SCIDB_VER defined, and SCIDB
# will not be in the same place... but the 3rd party directory *will*
# be, so build it using SCIDB_VER .
ifeq ($(SCIDB_VER),)
  SCIDB_3RDPARTY = $(SCIDB)
else
  SCIDB_3RDPARTY = /opt/scidb/$(SCIDB_VER)
endif

# A better way to set the 3rdparty prefix path that does not assume an
# absolute path...
ifeq ($(SCIDB_THIRDPARTY_PREFIX),)
  SCIDB_THIRDPARTY_PREFIX := $(SCIDB_3RDPARTY)
endif

INSTALL_DIR = $(SCIDB)/lib/scidb/plugins

# Include the OPTIMIZED flags for non-debug use
OPTIMIZED=-O3 -DNDEBUG -ggdb3 -g
STACKTRACE=-rdynamic
DEBUG=-g -ggdb3
CCFLAGS = -pedantic -W -Wextra -Wall -Wno-variadic-macros -Wno-strict-aliasing \
         -Wno-long-long -Wno-unused-parameter -Wno-unused -fPIC $(OPTIMIZED) $(STACKTRACE)
INC = -I. -DPROJECT_ROOT="\"$(SCIDB)\"" -I"$(SCIDB_THIRDPARTY_PREFIX)/3rdparty/boost/include/" \
      -I"$(SCIDB)/include" -I./extern

LIBS = -shared -Wl,-soname,libbc_between.so -ldl -L. \
       -L"$(SCIDB_THIRDPARTY_PREFIX)/3rdparty/boost/lib" -L"$(SCIDB)/lib" \
       -Wl,-rpath,$(SCIDB)/lib:$(RPATH)

SRCS = BCBetweenArray.cpp \
       LogicalBCBetween.cpp \
       PhysicalBCBetween.cpp

# Compiler settings for SciDB version >= 15.7
ifneq ("$(wildcard /usr/bin/g++-4.9)","")
 CC := "/usr/bin/gcc-4.9"
 CXX := "/usr/bin/g++-4.9"
 CCFLAGS+=-std=c++11 -DCPP11
else
 ifneq ("$(wildcard /opt/rh/devtoolset-3/root/usr/bin/gcc)","")
  CC := "/opt/rh/devtoolset-3/root/usr/bin/gcc"
  CXX := "/opt/rh/devtoolset-3/root/usr/bin/g++"
  CCFLAGS+=-std=c++11 -DCPP11
 endif
endif

all: libbc_between.so

clean:
	rm -rf *.so *.o

libbc_between.so: $(SRCS) BCBetweenArray.h
	@if test ! -d "$(SCIDB)"; then echo  "Error. Try:\n\nmake SCIDB=<PATH TO SCIDB INSTALL PATH>"; exit 1; fi
	$(CXX) $(CCFLAGS) $(INC) -o BCBetweenArray.o -c BCBetweenArray.cpp
	$(CXX) $(CCFLAGS) $(INC) -o LogicalBCBetween.o -c LogicalBCBetween.cpp
	$(CXX) $(CCFLAGS) $(INC) -o PhysicalBCBetween.o -c PhysicalBCBetween.cpp
	$(CXX) $(CCFLAGS) $(INC) -o libbc_between.so plugin.cpp BCBetweenArray.o LogicalBCBetween.o PhysicalBCBetween.o $(LIBS)
	@echo "Now copy libbc_between.so to $(INSTALL_DIR) on all your SciDB nodes, and restart SciDB."

test:
	./test.sh

