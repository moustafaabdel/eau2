

#include <assert.h>
#include "../src/store/store.h"
#include "../src/util/string.h"
#include "../src/dataframe/schema.h"
#include "../src/dataframe/dataframe.h"
#include "application.cpp"


/*********************************************************************************
 * Trival application to display adding a df to a kvs and pulling a df from a kvs.
*********************************************************************************/
class Trivial : public Application {
 public:
  Trivial(size_t idx) : Application(idx) { }
  void run_() override {
    size_t SZ = 1000000;
    int* vals = new int[SZ];
    int sum = 0;

    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    Key key("triv",0);

    DataFrame* df = DataFrame::fromArray(&key, kv, SZ, vals);

    assert(df->get_int(0,1) == 1);

    DataFrame* df2 = DataFrame::deserialize(kv->get(key).serialized_data); 

    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_int(0,i);

    // printf("%d\n", sum);

    printf(sum==0 ? "\nINTEGRATION TEST: SUCCESS\n\n" : "\nINTEGRATION TEST: FAILURE\n\n");
    delete df; delete df2;
  }


};

int main (int argc, char* argv[]) {
    KeyValueStore* kv = new KeyValueStore();
    Trivial simple_app(0);
    simple_app.assign_store(kv);
    simple_app.run_();
    // printf("\nM2 TRIVIAL APP PASSED!\n\n");
    return 0;
}
