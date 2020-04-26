
#include "../src/store/store.h"

class Application {
public:
    KeyValueStore* kv;
    size_t index;

    Application(size_t idx) {
        index = idx;
    }

    virtual void run_() {}

    void assign_store(KeyValueStore* store) {
        kv = store;
    }
};
