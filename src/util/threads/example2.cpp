//lang::Cpp

#include <stdio.h>
#include <thread>

int id = 0;

void foo() {
  printf("Hello from the thread no. %d\n", ++id);
}

int main(int argc, char **argv) {
  std::thread* pool[10];

  for (int i = 0; i < 10; i++) {
    pool[i] = new std::thread(foo);
  }

  printf("Hello from the main thread\n");
  
  for (int i = 0; i < 10; i++) {
    pool[i]->join();
  }

  return 0;
}

