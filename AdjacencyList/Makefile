CXX = g++
CXXFLAGS = -Wall -g
LFLAGS = -lpthread -std=c++17

all: main

main: main.o AdjacencyList.o mdlist.o
	$(CXX) $(CXXFLAGS) -O3 -o main main.o AdjacencyList.o mdlist.o $(LFLAGS)

main.o: main.cpp ThreadData.h
	$(CXX) $(CXXFLAGS) -c main.cpp $(LFLAGS)

AdjacencyList.o: AdjacencyList.cpp lftt.h pre_alloc.h
	$(CXX) $(CXXFLAGS) -c AdjacencyList.cpp $(LFLAGS)

mdlist.o: mdlist.cc lftt.h pre_alloc.h
	$(CXX) $(CXXFLAGS) -c mdlist.cc $(LFLAGS)

clean:
	rm -f main *.o
