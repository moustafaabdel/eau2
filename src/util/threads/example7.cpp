//lang::Cpp

#include <stdio.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>

std::condition_variable_any cv;
std::mutex cv_mtx;

void foo() {
  printf("foo: I'll wait for bar.\n");
  cv.wait(cv_mtx); // upon waking up cv_mtx is locked
  printf("foo: Done waiting.\n");
  cv_mtx.unlock();
}

void bar() {
  printf("bar: I'll sleep for 3 seconds\n");
  std::this_thread::sleep_for(std::chrono::seconds(3));
  printf("bar: Done sleeping.\n");
  cv.notify_all();
}

int main(int argc, char **argv) {
  std::thread t1(foo); // create a thread running foo
  std::thread t2(foo); // create a thread running foo
  std::thread t3(bar); // create a thread running bar

  t1.join();
  t2.join();
  t3.join();

  return 0;
}

