#pragma once
//lang::CwC

#include "../util/string.h"
#include "../util/helper.h"

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 ***********************************************************************/
class Schema : public Object {
public:

  size_t nrows;
  size_t ncols;
  char* col_types;

  /** Copying constructor */
  Schema(Schema& from) {
    nrows = from.nrows;
    ncols = from.ncols;
    col_types = from.col_types;
  }

  /** Create an empty schema **/
  Schema() {
    nrows = 0;
    ncols = 0;
    col_types = nullptr;
  }

  ~Schema() {
    delete[] col_types;
  }



  /** Create a schema from a string of types. A string that contains
    * characters other than those identifying the four type results in
    * undefined behavior. The argument is external, a nullptr argument is
    * undefined. **/
  Schema(const char* types) {
    nrows = 0;
    String* s = new String(types);
    col_types = s->c_str();
    ncols = s->size();
  }

//TODO: Update size using realloc and memcpy new val in
  /** Add a column of the given type. */
  void add_column(char typ, size_t col_size) {

    char* updated_col_types = new char[ncols + 1];

      for (int i = 0; i < ncols; i ++) {
          updated_col_types[i] = col_types[i];
      }
    updated_col_types[ncols] = typ;

    delete[] col_types;

    col_types = updated_col_types;
    ncols += 1;

    if (col_size > nrows) {
      nrows = col_size;
    }
  }

  /** Add a row with a name (possibly nullptr), name is external.  Names are
   *  expectd to be unique, duplicates result in undefined behavior. */
  void add_row() {
    nrows += 1;
  }

  /** Return type of column at idx. An idx >= width is undefined. */
  char col_type(size_t idx) {
      if (idx >= ncols) {
          return '\0';
      }
    return col_types[idx];
  }


  /** Returns the number of columns. */
  size_t width() {
    return ncols;
  }

  /** Returns the number of rows. */
  size_t length() {
    return nrows;
  }

  /** Returns the column types. */
  char* get_col_types() {
    return col_types;
  }


  /** Serializes this Schema into a char*. */
  // Currently does not support serializing or Deserializing column names and row names
  const char* serialize() {
      Serialize* serial = new Serialize();
      serial->serialize(nrows);
      serial->serialize(ncols);
      serial->serialize(ncols, col_types);
      return serial->buffer;
  }

  /** Constructs a Schema from a serialized char*. */
  static Schema* deserialize(const char* serialized_data) {
      Schema* new_schm = new Schema();

      new_schm->nrows = Deserialize::deserialize_size_t(serialized_data);
      new_schm->ncols = Deserialize::deserialize_size_t(serialized_data + sizeof(size_t));
      new_schm->col_types = Deserialize::deserialize(serialized_data + 2 * sizeof(size_t), new_schm->ncols);

      return new_schm;
  }
};
