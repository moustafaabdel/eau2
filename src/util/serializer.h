#pragma once
//lang::CwC

#include <cstring>
#include <string>
#include <cassert>
#include <stdarg.h>
#include <math.h>

/****************************************************************************
 * Serialize::
 *
 * Serializes & stores serialized values.
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 ***************************************************************************/
class Serialize {
public:
    char* buffer;
    size_t idx;
    size_t size;

    Serialize() {
        buffer = new char[1024];
        idx = 0;
        size = sizeof(buffer);
    }

    /** Serializes bool value. */
    const char* serialize(bool b) {
        size_t size = sizeof(bool);

        update_buffer(size);
        memcpy(buffer + idx, &b, size);

        idx += size;
        return buffer;
    }

    /** Serializes int value. */
    const char* serialize(int num) {
        size_t size = sizeof(int);

        update_buffer(size);
        memcpy(buffer + idx, &num, size);

        idx += size;
        return buffer;
    }

    /** Serializes float value. */
    const char* serialize(float f) {
        size_t size = sizeof(float);

        update_buffer(size);
        memcpy(buffer + idx, &f, size);

        idx += size;
        return buffer;
    }

    /** Serializes size_t value. */
    const char* serialize(size_t num) {
        size_t size = sizeof(size_t);

        update_buffer(size);
        memcpy(buffer + idx, &num, size);

        idx += size;
        return buffer;
    }

    /** Serializes char value. */
    //TODO: remove
    const char* serialize(char c) {
        size_t size = sizeof(char);

        update_buffer(size);
        memcpy(buffer + idx, &c, size);

        idx += size;
        return buffer;
    }

    /** Serializes char value. */
    //TODO: remove
    // const char* serialize(char* c) {
    //     size_t size = sizeof(char*);
    //
    //     update_buffer(size);
    //     memcpy(buffer + idx, &c, size);
    //
    //     idx += size;
    //     return buffer;
    // }

    /** Serializes char array. */
    const char* serialize(size_t len_array,  char* array) {
        for (size_t i = 0; i < len_array; i++) {
            serialize(array[i]);
        }
        idx += len_array;
        return buffer;
    }


    const char* serialize(size_t len_array,  const char* array) {
        for (size_t i = 0; i < len_array; i++) {
            serialize(array[i]);
        }
        idx += len_array;
        return buffer;
    }


    /** Serializes int array. */
    const char* serialize(size_t len_array, int* array) {
        update_buffer(len_array);
        memcpy(buffer + idx, &array, len_array);

        idx += len_array;
        return buffer;
    }

    const char* serialize(unsigned val) {
      size_t size = sizeof(unsigned);

      update_buffer(size);
      memcpy(buffer + idx, &val, size);

      idx += size;
      return buffer;
    }

    const char* serialize(short val) {
      size_t size = sizeof(short);

      update_buffer(size);
      memcpy(buffer + idx, &val, size);

      idx += size;
      return buffer;
    }

    const char* serialize(unsigned short val) {
      size_t size = sizeof(unsigned short);

      update_buffer(size);
      memcpy(buffer + idx, &val, size);

      idx += size;
      return buffer;
    }
    //
    // const char* serialize(unsigned long val) {
    //   size_t size = sizeof(unsigned long);
    //
    //   update_buffer(size);
    //   memcpy(buffer + idx, &val, size);
    //
    //   idx += size;
    //   return buffer;
    // }



    /** Updates size of buffer if needed. */
    void update_buffer(size_t size_of_element) {

        if (size_of_element + idx >= size) {
            char* updated_buf = new char[size * 2]; // (int)(ceil((size_of_element + idx) / sizeof(buffer)))];
            memcpy(updated_buf, buffer, size *2);
            buffer = updated_buf;
            size *= 2;
        }
    }

};


/****************************************************************************
 * Deserialize::
 *
 * Responsible for deserializing primitive values.
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 ***************************************************************************/
class Deserialize {
public:

    /** Deserializes char from serialized value. */
    static char deserialize_char(const char* val) {
        char tmp;
        memcpy(&tmp, val, sizeof(char));
        return tmp;
    }

    /** Deserializes bool from serialized value. */
    static bool deserialize_bool(const char* val) {
        bool tmp;
        memcpy(&tmp, val, sizeof(bool));
        return tmp;
    }

    /** Deserializes int from serialized value. */
    static int deserialize_int(const char* val) {
        int tmp;
        memcpy(&tmp, val, sizeof(int));
        return tmp;
    }

    /** Deserializes float from serialized value. */
    static float deserialize_float(const char* val) {
        float tmp;
        memcpy(&tmp, val, sizeof(float));
        return tmp;
    }

    /** Deserializes size_t from serialized value. */
    static size_t deserialize_size_t(const char* val) {
        size_t tmp;
        memcpy(&tmp, val, sizeof(size_t));
        return tmp;
    }

    /** Deserializes short from serialized value. */
    static size_t deserialize_short(const char* val) {
        short tmp;
        memcpy(&tmp, val, sizeof(short));
        return tmp;
    }

    /** Deserializes unsigned short from serialized value. */
    static size_t deserialize_unsignedshort(const char* val) {
        unsigned short tmp;
        memcpy(&tmp, val, sizeof(unsigned short));
        return tmp;
    }

    /** Deserializes unsigned short from serialized value. */
    static size_t deserialize_unsignedlong(const char* val) {
        unsigned long tmp;
        memcpy(&tmp, val, sizeof(unsigned long));
        return tmp;
    }

    /** Deserializes unsigned short from serialized value. */
    static size_t deserialize_unsigned(const char* val) {
        unsigned tmp;
        memcpy(&tmp, val, sizeof(unsigned));
        return tmp;
    }

    /** Deserializes char* from serialized value. */
    static char* deserialize(const char* msg, size_t len_arr) {
        char* c = new char[len_arr];
        memcpy(c, msg, len_arr);
        return c;
    }
};
