
EXECUTABLES=example1 example2 example3 example4 example5 example6 example7 prodcons
CXX=clang++
CXXFLAGS=-Wall --std=c++11 -g -O0

# Make targets

all: $(EXECUTABLES) 

example1: example1.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

example2: example2.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

example3: example3.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

example4: example4.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

example5: example5.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

example6: example6.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

example7: example7.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

prodcons: prodcons.cpp object.h string.h
	$(CXX) $(CXXFLAGS) -o $@ $<

clean: 
	rm -f $(EXECUTABLES) 
	rm -rf $(addsuffix .dSYM, $(EXECUTABLES))

.PHONY: comp all run run_timed

