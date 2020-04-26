

#include <assert.h>
#include "../src/store/store.h"
#include "../src/util/string.h"
#include "../src/dataframe/schema.h"
#include "../src/dataframe/dataframe.h"
#include "application.cpp"
#include <unistd.h>

/*************************************************************************
 * Demo application to display dataframe distribution over multiple nodes.
*************************************************************************/
class Demo : public Application {
public:
  Key main;
  Key verify;
  Key check;

  Demo(size_t idx): Application(idx), main("main",0), verify("verif",0), check("ck",0) {}

  void run_() override {
    switch(this_node()) {
    case 0:   producer();     break;
    case 1:   counter();      break;
    case 2:   summarizer();
   }
  }

  void producer() {
    printf("in producer on node %zu\n", index);
    size_t SZ = 100*1000;
    int* vals = new int[SZ];
    int sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;


    DataFrame::fromArray(&main, kv, SZ, vals);
    DataFrame::fromScalar(&check, kv, sum);

    kv->run(); //Move outside -> run a thread for KV store before application thread
  }

  void counter() {

    printf("in counter on node %zu\n", index);
    DataFrame* v = DataFrame::deserialize(kv->waitAndGet(main).serialized_data);
    int sum = 0;
    for (size_t i = 0; i < 100*1000; ++i) sum += v->get_int(0,i);
    printf("The sums is %d\n", sum);
    DataFrame::fromScalar(&verify, kv, sum);
  }

  void summarizer() {
    printf("in summarizer on node %zu\n", index);
    DataFrame* result = DataFrame::deserialize(kv->waitAndGet(verify).serialized_data);
    DataFrame* expected = DataFrame::deserialize(kv->waitAndGet(check).serialized_data);
    printf("%s\n", expected->get_int(0,0)==result->get_int(0,0) ? "SUCCESS":"FAILURE");
  }

  int this_node() {
    return index;
  }
};

typedef struct thread_struct {
    int id;
    KeyValueStore* store;
}struct_t;

void* distribute(void* args) {
  struct_t* arguments = (struct_t*) args;
  int index = arguments->id;
  KeyValueStore* kv = arguments->store;

  Demo d(index);

  d.assign_store(kv);

  d.run_();

  return nullptr;
}

// M3 pseudo-network approach to run multiple KV stores in the same process in different threads
int main (int argc, char* argv[]) {

  // //spawn 3 threads to run separate client applications and kvs for demo.
  // int num_threads = 3;
  // pthread_t tids[num_threads];

  // // create threads while passing in node id to be used by application.
  // for(int k = 0; k < num_threads; k++){
  //     struct_t* arguments = (struct_t*)(malloc(sizeof(struct_t)));
  //     arguments->id = k;
  //     arguments->store = new KeyValueStore(k, 8000+k, num_threads);

  //     pthread_create(&tids[k], NULL, distribute, arguments);
  // }

  // for(int l = 0; l < num_threads; l++){
  //     pthread_join(tids[l],NULL);
  // }

  KeyValueStore* kv;
  int id;
  int port;
  int nodes;

  if (argc > 3) {
    id = atoi(argv[1]);
    port = atoi(argv[2]);
    nodes = atoi(argv[3]);
    kv = new KeyValueStore(id, port, nodes);
  }
  else {
    id = 0;
    kv = new KeyValueStore();
  }

  Demo d(id);

  d.assign_store(kv);

  d.run_();

  //usleep(1000000*5);
  //delete kv;

  // Create a new KVS
  // KeyValueStore* kvs = new KeyValueStore();

  // // Create the Application
  // Demo d(0);

  // // Add registration

  // // Communication between nodes

  // // Add method to pass in kvs
  // d.add_kvs(kvs);

  // // Run the application
  // d.run_();

  // Wait for everything to finish


  printf("M3 DEMO TEST PASSED!\n\n");
}
