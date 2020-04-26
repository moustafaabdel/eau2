//lang::CwC
#pragma once

#include "../util/object.h"
#include "../util/string.h"

class BoolColumn;
class IntColumn;
class FloatColumn;
class StringColumn;

/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality.
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu.
 * ************************************************************************/
class Column : public Object {
public:

  virtual ~Column() {}

  /** Type converters: Return same column under its actual type, or
   *  nullptr if of the wrong type.  */
  virtual BoolColumn* as_bool() { return nullptr; }
  virtual IntColumn* as_int() { return nullptr; }
  virtual FloatColumn* as_float() { return nullptr; }
  virtual StringColumn* as_string() { return nullptr; }

  /** Type appropriate push_back methods. Calling the wrong method is
    * undefined behavior. **/
  virtual void push_back(bool val){}
  virtual void push_back(int val){}
  virtual void push_back(float val){}
  virtual void push_back(String* val){}

 /** Returns the number of elements in the column. */
  virtual size_t size(){return 0;}


  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
  char get_type() {
    if (as_bool() != nullptr) {
      return 'B';
    }
    else if (as_int() != nullptr) {
      return 'I';
    }
    else if (as_float() != nullptr) {
      return 'F';
    }
    else if (as_string() != nullptr) {
      return 'S';
    }
    else {
      return '\0';
    }
  }


};

/**************************************************************************
 * BoolColumn ::
 * Represents one column of a data frame which holds values of type bool.
 * This column is mutable, equality is pointer
 * equality.
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu.
 * ************************************************************************/
class BoolColumn : public Column {
public:
    size_t len_;
    size_t size_;
    bool* elements_;


    BoolColumn() {
        len_ = 0;
        size_ = 0;
        elements_ = nullptr;
    }

    virtual ~BoolColumn() {
       delete[] elements_;
    }

    /** Returns boolean value at specified index. */
    bool get(size_t idx) {
        assert(idx <= len_);
        return elements_[idx];
    }

    /** Returns this BoolColumn. */
    virtual BoolColumn* as_bool() {
        return this;
    }

    /** Returns number of elements stored in this BoolColumn. */
    virtual size_t size() {
        return len_;
    }

    /** Adds desired bool value to end of current array of elements.*/
    virtual void push_back(bool val) {
        update_size();
        memcpy(elements_ + len_, &val, sizeof(bool) );
        len_ += 1;
    }

    /** Reallocates size of elements_ if needed. */
    void update_size() {
      if (size_ == 0) {
        elements_ = (bool*)realloc(elements_, 1024);
        size_ += 1024;
      }
      else if (len_ * sizeof(bool) >= size_ - sizeof(bool)) {
        elements_ = (bool*)realloc(elements_, size_ * 2);
        size_ *= 2;
      }
    }

    /** Serializes this BoolColumn into a char*. */
    const char* serialize() {

        Serialize* serial = new Serialize();
        serial->serialize(len_);
        for (int i = 0; i < len_; i++) {
            serial->serialize(elements_[i]);
        }

        return serial->buffer;
    }

    /** Constructs a BoolColumn from a serialized char*. */
    static BoolColumn* deserialize(const char* msg) {
        size_t len_arr = Deserialize::deserialize_size_t(msg);

        BoolColumn* col = new BoolColumn();
        for (int i = 0; i < len_arr; i++) {
            bool a = Deserialize::deserialize_bool(msg + sizeof(size_t) + i * sizeof(bool));
            col->push_back(a);
        }

        return col;
    }

};


/**************************************************************************
 * IntColumn ::
 * Represents one column of a data frame which holds values of type int.
 * This column is mutable, equality is pointer
 * equality.
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu.
 * ************************************************************************/
class IntColumn : public Column {
public:
    size_t len_;
    size_t size_;
    int* elements_;

    IntColumn() {
        len_ = 0;
        size_ = 0;
        elements_ = nullptr;
    }

    virtual ~IntColumn() {
       delete[] elements_;
    }

    /** Returns int value at specified index. */
    int get(size_t idx) {
        assert(idx <= len_);
        return elements_[idx];
    }

    /** Returns this IntColumn. */
    virtual IntColumn* as_int() {
        return this;
    }

    /** Returns number of elements stored in this IntColumn. */
    virtual size_t size() {
        return len_;
    }

    /** Adds desired bool value to end of current array of elements.*/
    virtual void push_back(bool val) {
        int bool_val = static_cast<int>(val);
        push_back(bool_val);
    }

    /** Adds desired int value to end of current array of elements.*/
    virtual void push_back(int val) {
        update_size();
        memcpy(elements_ + len_, &val, sizeof(int));
        len_ += 1;
    }

    /** Reallocates size of elements_ if needed. */
    void update_size() {
      if (size_ == 0) {
        elements_ = (int*)realloc(elements_, 1024);
        size_ += 1024;
      }
      else if (len_ * sizeof(int) >= size_ - sizeof(int)) {
        elements_ = (int*)realloc(elements_, size_ * 2);
        size_ *= 2;
      }
    }


    /** Serializes this IntColumn into a char*. */
    const char* serialize() {

        Serialize* serial = new Serialize();
        serial->serialize(len_);

        for (int i = 0; i < len_; i++) {
            serial->serialize(elements_[i]);
        }

        return serial->buffer;
    }

    /** Constructs an IntColumn from a serialized char*. */
     static IntColumn* deserialize(const char* msg) {
        size_t len_arr = Deserialize::deserialize_size_t(msg);

        IntColumn* col = new IntColumn();
        for (int i = 0; i < len_arr; i++) {
            int a = Deserialize::deserialize_int(msg + sizeof(size_t) + i * sizeof(int));
            col->push_back(a);
        }
        return col;
    }

};


/**************************************************************************
 * FloatColumn ::
 * Represents one column of a data frame which holds values of type float.
 * This column is mutable, equality is pointer
 * equality.
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu.
 * ************************************************************************/
class FloatColumn : public Column {
public:
    size_t len_;
    size_t size_;
    float* elements_;

    FloatColumn() {
        len_ = 0;
        size_ = 0;
        elements_ = nullptr;
    }


    virtual ~FloatColumn() {
       delete[] elements_;
    }

    /** Returns float value at specified index. */
    float get(size_t idx) {
        assert(idx <= len_);
        return elements_[idx];
    }

    /** Returns this FloatColumn. */
    virtual FloatColumn* as_float() {
        return this;
    }

    /** Returns number of elements stored in this FloatColumn. */
    virtual size_t size() {
        return len_;
    }

    /** Adds desired bool value to end of current array of elements.*/
    virtual void push_back(bool val) {
        float bool_val = static_cast<float>(val);
        push_back(bool_val);
    }

    /** Adds desired int value to end of current array of elements.*/
    virtual void push_back(int val) {
        float int_val = static_cast<float>(val);
        push_back(int_val);
    }

    /** Adds desired float value to end of current array of elements.*/
    virtual void push_back(float val) {
        update_size();
        memcpy(elements_ + len_, &val, sizeof(float));
        len_ += 1;
    }

    /** Reallocates size of elements_ if needed. */
    void update_size() {
      if (size_ == 0) {
        elements_ = (float*)realloc(elements_, 1024);
        size_ += 1024;
      }
      else if (len_ * sizeof(float) >= size_ - sizeof(float)) {
        elements_ = (float*)realloc(elements_, size_ * 2);
        size_ *= 2;
      }
    }

    /** Serializes this FloatColumn into a char*. */
     const char* serialize() {

        Serialize* serial = new Serialize();
        serial->serialize(len_);

        for (int i = 0; i < len_; i++) {
            serial->serialize(elements_[i]);
        }

        return serial->buffer;
    }

    /** Constructs a FloatColumn from a serialized char*. */
     static FloatColumn* deserialize(const char* msg) {
        size_t len_arr = Deserialize::deserialize_size_t(msg);

        FloatColumn* col = new FloatColumn();
        for (int i = 0; i < len_arr; i++) {
            float a = Deserialize::deserialize_float(msg + sizeof(size_t) + i * sizeof(float));
            col->push_back(a);
        }

        return col;
    }


};


/**************************************************************************
 * StringColumn ::
 * Represents one column of a data frame which holds values of type String.
 * This column is mutable, equality is pointer
 * equality. Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu.
 * ************************************************************************/
class StringColumn : public Column {
public:
    size_t len_;
    size_t size_;
    String** elements_;

    StringColumn() {
        len_ = 0;
        size_ = 0;
        elements_ = nullptr;
    }

    virtual ~StringColumn() {
       delete[] elements_;
    }

    /** Returns String value at specified index. */
    String* get(size_t idx) {
        return elements_[idx];
    }

    /** Returns this StringColumn. */
    virtual StringColumn* as_string() {
        return this;
    }

    /** Returns number of elements stored in this StringColumn. */
    virtual size_t size() {
        return len_;
    }

    /** Adds desired String value to end of current array of elements.*/
    virtual void push_back(String* val) {

        update_size();
        memcpy(elements_ + len_, &val, val->num_bytes());
        len_ += 1;
    }

    /** Reallocates size of elements_ if needed. */
    void update_size() {
      if (size_ == 0) {
        elements_ = (String**)realloc(elements_, 1024);
        size_ += 1024;
      }
      else if (len_ * (sizeof(size_t) + sizeof(char*)) >= size_ - (sizeof(size_t) + sizeof(char*))) {
        elements_ = (String**)realloc(elements_, size_ * 2);
        size_ *= 2;
      }
    }

    /** Serializes this StringColumn into a char*. */
    const char* serialize() {

         Serialize* serial = new Serialize();
         serial->serialize(len_);

         for (int i = 0; i < len_; i++) {
            serial->serialize(elements_[i]->size_);
            serial->serialize(elements_[i]->size_, elements_[i]->cstr_);
         }

         return serial->buffer;
    }

    /** Constructs a StringColumn from a serialized char*. */
     static StringColumn* deserialize(const char* msg) {
         size_t len_arr = Deserialize::deserialize_size_t(msg);
         StringColumn* col = new StringColumn();

         //Create an index with starting value of size len_arr
         size_t bytes_read = sizeof(size_t);

         for (int i = 0; i < len_arr; i++) {
            String* current_string = String::deserialize(msg + i * sizeof(size_t) + bytes_read);
            col->push_back(current_string);
            bytes_read += current_string->size() * 2;
         }

         return col;
    }

};
