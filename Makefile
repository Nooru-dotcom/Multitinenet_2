CXX      = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra -pthread
LDFLAGS  = -lpq -pthread
PG_INC   = $(shell pg_config --includedir 2>/dev/null || echo /usr/include/postgresql)

SRC      = src
TESTS    = tests

.PHONY: all clean test

all: tenant-proxy tenant-cli

tenant-proxy: $(SRC)/tenant-proxy.cpp \
              $(SRC)/config.hpp \
              $(SRC)/rewriter.hpp \
              $(SRC)/sessions.hpp \
              $(SRC)/pool.hpp \
              $(SRC)/backend.hpp \
              $(SRC)/formatter.hpp \
              $(SRC)/metrics.hpp
	$(CXX) $(CXXFLAGS) -I$(SRC) -I$(PG_INC) -o $@ $(SRC)/tenant-proxy.cpp $(LDFLAGS)

tenant-cli: $(SRC)/tenant-cli.cpp
	$(CXX) $(CXXFLAGS) -I$(SRC) -o $@ $(SRC)/tenant-cli.cpp

$(TESTS)/unit_test: $(TESTS)/unit_test.cpp \
                    $(SRC)/rewriter.hpp \
                    $(SRC)/config.hpp
	$(CXX) $(CXXFLAGS) -I$(SRC) -I$(PG_INC) -o $@ $(TESTS)/unit_test.cpp

test: $(TESTS)/unit_test
	./$(TESTS)/unit_test

clean:
	rm -f tenant-proxy tenant-cli $(TESTS)/unit_test
