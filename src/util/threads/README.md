# Concurrency examples

This directory contains:

(a) examples used in the class on concurrency in the week starting February 3rd
    (`example1.cpp` to `example7.cpp`)
(b) an implementation of a simple thread library (`thread.h`), and
(c) a Producer/Consumer demo using the above library (`prodcons.cpp`).

The files in (a) exemplify primitives and operations provided by C++11 and
above for implementing concurrent programs. These operations and primitives are
used in the implementation of (b). 

The default Makefile target compiles everything:

```
$ make
```

You can compile and run individual examples using

```
$ make exampleX
$ ./exampleX
```

for `X = {1..7}`.

The producer/consumer example can be compiled and run using

```
$ make prodcons
$ ./prodcons
```

