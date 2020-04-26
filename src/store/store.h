//lang::CwC
#pragma once
#include <iostream>
#include <iterator>
#include <assert.h>
#include <map>
#include "../util/string.h"
#include "../network/new/message.h"
#include "../network/new/network_ip.h"

#include "../util/threads/thread.h"


using namespace std;

/****************************************************************************
 * Key::
 *
 * Represents a unique identifier of a value.
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 ***************************************************************************/
class Key {
public:
    String* name;
    size_t home;

    /** Constructs a key using a specified key name (String) and a home node. */
    Key(String* n, size_t h) {
        name = n;
        home = h;
    }

    /** Constructs a key using a specified key name (char*) and a home node. */
    Key(const char* n, size_t h) {
        name = new String(n);
        home = h;
    }

    Key() {}

    ~Key() {}

    size_t sizeof_serialized() {
        size_t size = sizeof(size_t) * 2 + name->size() * sizeof(char) * 2;
        return size;
    }

    const char* serialize() {
        Serialize* s = new Serialize();
        s->serialize(home);
        s->serialize(name->size());
        s->serialize(name->size(), name->c_str());

        return s->buffer;
    }

    static Key* deserialize(const char* buffer) {
        size_t buff_size = 0;

        size_t home_idx = Deserialize::deserialize_size_t(buffer);
        buff_size += sizeof(size_t);

        String* s = String::deserialize(buffer + buff_size);

        Key* key = new Key(s, home_idx);

        return key;
    }
};

// Used as a comparator for map
struct cmp_keys
{
    bool operator()(Key k1, Key k2) const
    {
        return k1.name->compare(k2.name) < 0;
    }
};


/****************************************************************************
 * Value::
 *
 * Represents serialized data. (In this case a serialized DF)
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 ***************************************************************************/
class Value {
public:
    const char* serialized_data;
    size_t size;

    /** Constructs a value using a serialized df (char*). */
    Value(const char* s_d, size_t s) {
        serialized_data = s_d;
        size = s;
    }

    Value(const char* s_d) {
        serialized_data = s_d;
        size = 0;
    }

    Value() {}

    ~Value() {}


    size_t sizeof_serialized() {
        return size;
    }

    const char* serialize() {
        return serialized_data;
    }

    static Value* deserialize(const char* buffer) {
        const char* s_d = buffer;

        Value* val = new Value(s_d);

        return val;
    }
};



/****************************************************************************
 * Value::
 *
 * Represents a set of key value pairs.
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 ***************************************************************************/
class KeyValueStore :public Thread {
public:

    // Instance of network to communicate
    NetworkIP* network;
    // pthread_t tids[1];

    //Assumption: all key names in a kvs are unique
    map<Key, Value, cmp_keys> kv;

    KeyValueStore(int idx, int port, int num_nodes) {
        network = new NetworkIP();
        if (idx == 0) {
            network->server_init(idx, 8000, num_nodes);
        }
        else {
            network->client_init(idx, port, (char*)"127.0.0.1", 8000, num_nodes);
        }

        //spawn thread with a loop that waits on requests and serves them

        // create threads while passing in node id to be used by application.
        // arg_t arguments;
        // arguments.store = kv;
        // arguments.network_ip = network;

        // pthread_create(&tids[0], NULL, serve, (void*)&arguments);
    }

    KeyValueStore() {
        network = new NetworkIP();
        //network->server_init(0, 8000, 1);
    }

    ~KeyValueStore() {
    }

    /** Get a value from this kv store using a specified key. */
    Value get(Key k) {
      // // Is this the home of this key
      if (network->index() == k.home) {
        if (kv.count(k) > 0) {
            return kv.at(k);
        }
        else {
            Value nf("NOT FOUND\0");
            return nf;
        }
      }
      // If not
      else {
        // send message to owner
        // wait on a reply
        // Return get value using home node matching with key home node
        Message* request = new Message(network->index(), k.home, 'K', k.serialize(), k.sizeof_serialized());
        network->send_message(request);
        //send msg with serialize key from node 1 to node 2
        //node 1 deserializes value and return
        Message* response = network->recv_m();

        return *Value::deserialize(response->payload);
      }
    }

    /** Add a key value pair to this kv store. */
    bool put(Key k, Value v) {
        kv.insert(std::pair<Key,Value>(k,v));

        //printf("inserted %s on node %zu\n", k.name->c_str(), k.home);
        return true;
    }

    /** Waits for a non-null return value and returns that value. */
    Value waitAndGet(Key k) {
        // wait and notify methods found in thread class

        Value v = get(k);


        while (strcmp(v.serialized_data, "NOT FOUND\0") == 0) {
            // printf("waitAndGet NOT FOUND\n");
          v = get(k);
        }
        return v;
    }

    void run() {
        while (1) {
            //node 2 deserializes key, gets value from its kv store
            Message* req = network->recv_m();
            Key* k = Key::deserialize(req->payload);
            printf("Key name: %s\n", k->name->c_str());
            printf("Key home: %zu\n", k->home);

            Value val = waitAndGet(*k);
            printf("Value :%s\n", val.serialized_data);
            // //node 2 serializes value and and send msg
            // Message* resp = new Message(network->index(), req->src, 'V', val.serialize(), val.sizeof_serialized());
            // network->send_message(resp);
        }
    }
};



//
// class Request : public Message {
//     Key k;
//
//     Request(unsigned sender, unsigned receiver, Key key) {
//         type = 'K';
//         src = sender;
//         dest = receiver;
//         payload = " ";
//         k = key;
//     }
//
//     ~Request() {}
//
//     const char* serialize() {
//         printf("In REQ Serialize\n");
//         Serialize* serial = new Serialize();
//         serial->serialize(type);
//         serial->serialize(src);
//         serial->serialize(dest);
//
//         serial->serialize(k.home);
//         serial->serialize(k.name->size(), k.name->c_str());
//
//         return serial->buffer;
//     }
//
//     size_t sizeof_serialized() {
//         return (2 * sizeof(unsigned)) + sizeof(char) * 2
//         + sizeof(size_t) + k.name->size() * sizeof(char) * 2;
//     }
//
//     static Request* deserialize(const char* msg) {
//         size_t buff_size = 0;
//
//         char type = Deserialize::deserialize_char(msg);
//         buff_size += sizeof(type);
//
//         unsigned src = Deserialize::deserialize_unsigned(msg + buff_size);
//         buff_size += sizeof(src);
//
//         unsigned dest = Deserialize::deserialize_unsigned(msg + buff_size);
//         buff_size += sizeof(dest);
//
//         Key* key = Key::deserialize(msg + buff_size);
//
//         Request* message = new Request(src, dest, *key);
//
//         return message;
//
//     }
// };
