all: xing

CXX = g++

SRC = $(shell find src -type f -name '*.cpp')
LIBS = -lboost_system -lboost_thread -lglog
FLAGS = -std=c++11 -pthread -g -O3

xing:
	$(CXX) -o $@.bin $(SRC) $(LIBS) $(FLAGS)

clean:
	rm -rf *.bin *.bin.*
