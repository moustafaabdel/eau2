
#include <assert.h>
#include "../../src/util/string.h"
#include "../../src/store/store.h"


int main (int argc, char* argv[]) {

  String* simple_key = new String("SimpleKey");
  size_t node0 = 0;
  Key simple_k(simple_key, node0);

  char* c = new char[1];
  c[0] = 'A';

  Value simple_val(c);

  KeyValueStore* kvs = new KeyValueStore();
  kvs->put(simple_k, simple_val);
  const char* val = kvs->get(simple_k).serialized_data;

  assert(c[0] == val[0]);

  printf("Basic KVS Test Passed.\n");


  Key simple_k2(simple_key, 0);

  char* c_2 = new char[1];
  c_2[0] = 'C';

  Value simple_val_2(c_2);

  kvs->put(simple_k2, simple_val_2);
  const char* val_2 = kvs->get(simple_k2).serialized_data;

  //Does not update value of duplicate key (name).
  assert (c[0] == val_2[0]);

  printf("Duplicate KVS Test Passed.\n");

  String* complex_key = new String("ComplexKey");
  Key complex_k(complex_key, node0);

  char* long_char = new char[5];
  long_char[0] = 'A';
  long_char[1] = 'B';
  long_char[2] = 'C';
  long_char[3] = 'D';
  long_char[4] = 'E';

  Value long_val(long_char);

  kvs->put(complex_k, long_val);
  const char* complex_val = kvs->get(complex_k).serialized_data;

  assert(long_char == complex_val);

  // printf("Complex KVS Test Passed.\n");


  String* fake_key = new String("fakeKey");
  Key fake(fake_key, node0);

  Value value = kvs->get(fake);
  assert(strcmp(value.serialized_data, "NOT FOUND\0") == 0);

  // printf("Fake Key Test Passed.\n");
  size_t num_tests = 5;

  printf("\nKeyValueStore: All %zu KVS tests passed.\n\n", num_tests);


}
