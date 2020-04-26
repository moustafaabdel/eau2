// lang::Cpp

#include <stdio.h>
#include <mutex>
#include <thread>

int counter = 0;
std::mutex counter_mtx;

void foo() {
  for (int i = 0; i < 10000000; i++) {
    counter_mtx.lock();
    counter++;
    counter_mtx.unlock();
  }
}

int main(int argc, char **argv) {
  printf("Counter at the beginning: %d\n", counter);

  std::thread t1(foo); // create two threads running foo
  std::thread t2(foo);

  t1.join(); // wait for both threads to finish
  t2.join();

  printf("Final counter: %d\n", counter);

  return 0;
}

