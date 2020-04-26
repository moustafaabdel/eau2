// lang::Cpp

#include <stdio.h>
#include <thread>
#include <chrono>

void foo() {
  printf("foo: Sleeping for 2 seconds...\n");
  std::this_thread::sleep_for(std::chrono::seconds(2));
  printf("foo: Done sleeping.\n");
}

int main(int argc, char **argv) {
  std::thread t1(foo); // create two threads running foo
  std::thread t2(foo);
  
  printf("main: Sleeping for 1 second...\n");
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  printf("main: Done sleeping.\n");

  t1.join(); // wait for both threads to finish
  t2.join();

  return 0;
}

