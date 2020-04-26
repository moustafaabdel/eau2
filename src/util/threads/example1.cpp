//lang::Cpp

#include <stdio.h>
#include <thread>

void foo() {
  printf("Hello from the foo thread\n");
}

int main(int argc, char **argv) {
  std::thread t1(foo); // start foo() in a separate thread

  printf("Hello from the main thread\n");
  
  t1.join(); // wait for t1 to finish

  return 0;
}
