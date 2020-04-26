//lang::Cpp

#include <stdio.h>
#include <thread>
#include <atomic>

std::atomic<int> counter(0);

void foo() {
  for (int i = 0; i < 10000000; i++) {
    counter++;
  }
}

int main(int argc, char **argv) {
  printf("Counter at the beginning: %d\n", counter.load());

  std::thread t1(foo); // create two threads running foo
  std::thread t2(foo);

  t1.join(); // wait for both threads to finish
  t2.join();

  printf("Final counter: %d\n", counter.load());

  return 0;
}

