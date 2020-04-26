// lang::CwC

/** Producer/consumer example using our Thread library.
 *
 * @author Ferd <f.vesely@northeastern.edu>
 */


#include <time.h>

#include "string.h"
#include "thread.h"

/** A trivial Dropbox implementation for storing string messages. Only allows
 *  storing one message at a time.
 */
class Dropbox : public Object {
  public:
    String *message_; /** Message store. */
    bool empty_;      /** Emptyness indicator. */
    Lock lock_;

    /** Construct an empty Dropbox. */
    Dropbox() : message_(nullptr), empty_(true) {}

    /** Drop a message. */
    void drop(String *message) {
      lock_.lock(); // Acquire the lock - we need a consistent view of 
                    // empty_ AND message_
      while (!empty_) { // While the Dropbox IS NOT empty, wait
        lock_.wait();   // Remember we can only drop one message at a time
      }
      // At this point, the current thread is guaranteed to hold the lock so
      // the next two steps are done atomically.
      message_ = message; // Set the message
      empty_ = false;     // Indicate that the box is not empty anymore.
      lock_.unlock();    // Release the lock so it can be acquired by a "picker"
      lock_.notify_all(); // Notify any threads waiting to use this box
    }

    /** Pick the message in this Dropbox up. */
    String* pick() {
      lock_.lock(); // Acquire the lock - we will be checking for emptiness
      while (empty_) {  // While the box is empty...
        lock_.wait();   // ... wait
      }
      // At this point, the current thread holds the lock so the next two steps
      // are performed atomically.
      empty_ = true; // We are emptying the box
      String *msg = message_; // Save the pointer, because after we unloxk, 
                              // message_ might be pointing to a different 
                              // message
      lock_.unlock();  // Release the lock - a "dropper" can use the box
      lock_.notify_all(); // Notify any "droppers" that we picked up the message
      return msg;
    }
};

/** The producer thread. */
class Producer : public Thread {
  public:
    Dropbox *box_;

    Producer(Dropbox* box) : box_(box) { }

    /** Once started, the producer will try gradually dropping 5 messages. */
    virtual void run() {
      // A few messages to drop in the box
      const char* messages[] = {
        "Lorem ipsum dolor sit amet, orci nec diam.",
        "Pulvinar maecenas, sed ornare.",
        "Maecenas ut, sed praesent faucibus, ipsum pede.",
        "Est ipsum eget, cras aliquam.",
        "Donec sed aenean, aenean quis."
      };

      // Drop 5 messages in a row, but sleep an increasing amount of time
      // after each drop off.
      for (int i = 0; i < 5; i++) {
        String *str = new String(messages[i]);
        box_->drop(str);
        sleep(i * 100);
      }
      // Once we are done, signal so by dropping a null message.
      box_->drop(nullptr);
    }
};

/** The consumer thread. */
class Consumer : public Thread {
  public:
    Dropbox *box_;

    Consumer(Dropbox* box) : box_(box) { }

    /** Once started, the consumer will try retrieving the messages left by
     *  the producer.
     */
    virtual void run() {
      String* message = box_->pick(); // get the 1st message

      size_t sleep_time = 1500; // 
      while (message) { // while we didn't get the dummy null message
        printf("Message: %s\n", message->c_str()); // print the current message
        delete message; // release the message's memory
        sleep(sleep_time); // sleep for a decreasing amount of time
        sleep_time -= 300;
        message = box_->pick(); // get the next message
      }
    }
};

int main() {
  Dropbox *box = new Dropbox();
  // The producer and consumer share the same box
  Producer* p = new Producer(box);
  Consumer* c = new Consumer(box);

  // Start both
  printf("** Starting the producer and the consumer **\n");
  p->start();
  c->start();

  // Wait for both to finish
  p->join();
  c->join();
  printf("** Producer and consumer finished. Exiting **\n");

  return 0;
}

