#pragma once
//lang::CwC
#include <cstdlib>
#include "schema.h"
 #include "fielder.h"
#include "../util/helper.h"
#include "../util/object.h"
#include "../util/string.h"


/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 ************************************************************************/
class Row : public Object {
 public:

  char* col_types;
  void** elements_;
  size_t idx_;
  size_t width_;

  /** Build a row following a schema. */
  Row(Schema& scm) {
    col_types = scm.col_types;
    elements_ = new void*[scm.ncols];
    idx_ = scm.nrows;
    width_ = scm.ncols;
  }



  /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type is undefined. */
  void set(size_t col, int val) {
    assert(col_types[col] == 'I' || col_types[col] == 'B');
    int* val_ptr = new int[1];
    val_ptr[0] = val;
    elements_[col] = val_ptr;
  }
  void set(size_t col, float val) {
    assert(col_types[col] == 'I' || col_types[col] == 'B' || col_types[col] == 'F');
    float* val_ptr = new float[1];
    val_ptr[0] = val;
    elements_[col] = val_ptr;
  }
  void set(size_t col, bool val) {
    assert(col_types[col] == 'B');
      bool* val_ptr = new bool[1];
      val_ptr[0] = val;
      elements_[col] = val_ptr;
  }
  void set(size_t col, String* val) {
    assert(col_types[col] == 'S');
    String** val_ptr = new String*[1];
    val_ptr[0] = val;
    elements_[col] = val_ptr;
  }

  /** Set/get the index of this row (ie. its position in the dataframe. This is
   *  only used for informational purposes, unused otherwise */
  void set_idx(size_t idx) {
    idx_ = idx;
  }
  size_t get_idx() {
    return idx_;
  }

  /** Getters: get the value at the given column. If the column is not
    * of the requested type, the result is undefined. */
  int get_int(size_t col) {
    assert(col_types[col] == 'I');
    int* intptr = (int*)elements_[col];
    return intptr[0];
  }

  bool get_bool(size_t col) {
    assert(col_types[col] == 'B');
    bool* temp = (bool*)elements_[col];
    return temp[0];
  }

  float get_float(size_t col) {
    assert(col_types[col] == 'F');
    float* temp = (float*)elements_[col];
    return temp[0];
  }

  String* get_string(size_t col) {
    assert(col_types[col] == 'S');
    String** temp = (String**)elements_[col];
    return temp[0];
  }

  /** Number of fields in the row. */
  size_t width() {
    return width_;
  }

   /** Type of the field at the given position. An idx >= width is  undefined. */
  char col_type(size_t idx) {
    assert(idx < width_);
    return col_types[idx];
  }

  /** Given a Fielder, visit every field of this row. The first argument is
    * index of the row in the dataframe.
    * Calling this method before the row's fields have been set is undefined. */
  void visit(size_t idx, Fielder& f) {
      idx_ = idx;
      for (int i = 0; i < width(); i++) {
        f.accept(elements_[i]);
      }
  }

};
