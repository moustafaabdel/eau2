#pragma once
//lang::CwC
#include "schema.h"
#include "column.h"
#include "row.h"
#include "rower.h"
#include "../network/new/message.h"
#include "../store/store.h"

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 ****************************************************************************/
class DataFrame : public Object {
public:
  Schema* schema_;
  Column** columns_;

  /** Create Schema with empty dataframe*/
  DataFrame() {
    schema_ = new Schema();
    columns_ = nullptr;
  }

  /** Create a data frame from a schema and columns. All columns are created empty. */
  DataFrame(Schema& schema) {
    size_t size_of_schema = schema.width();
    schema_ = &schema;
    columns_ = new Column*[size_of_schema];
    for (int i = 0; i < size_of_schema; i++) {
        Column* col = create_col(schema.col_type(i));
        columns_[i] = col;
    }
  }

  /** Create a data frame with the same columns as the given df but with no rows or rownames */
  DataFrame(DataFrame& df) {
    schema_ = new Schema(*df.schema_);
    columns_ = nullptr;
    schema_->nrows = 0;
  }


  virtual ~DataFrame() {
      delete[] columns_;
  }

  /** Constructs a Column* using a specified column type. */
  Column* create_col(char col_type) {

    if (col_type == 'B') {
      return new BoolColumn();
    }
    else if (col_type == 'I') {
      return new IntColumn();
    }
    else if (col_type == 'F') {
      return new FloatColumn();
    }
    else if (col_type == 'S') {
      return new StringColumn();
    }
    else {
      return new Column();
    }
  }


  /** Returns the dataframe's schema. Modifying the schema after a dataframe
    * has been created in undefined. */
  Schema& get_schema() { return *schema_;}


  /** Adds a column this dataframe, updates the schema, the new column
    * is external, and appears as the last column of the dataframe, the
    * name is optional and external. A nullptr column is undefined. */
  void add_column(Column* col, String* name) {
    size_t col_size = 0;
    char type = col->get_type();
    if (type == 'B') {
        col_size = col->as_bool()->len_;
    }
    else if (type == 'I') {
        col_size = col->as_int()->len_;
    }
    else if (type == 'F') {
        col_size = col->as_float()->len_;
    }
    else if (type == 'S') {
        col_size = col->as_string()->len_;
    }

    schema_->add_column(col->get_type(), col_size);
    Column** updated_columns = new Column*[schema_->ncols];

    for (int i = 0; i < schema_->ncols - 1; i++) {
        updated_columns[i] = columns_[i];
    }
    updated_columns[schema_->ncols - 1] = col;
    delete[] columns_;

    columns_ = updated_columns;
  }

  /** Return the value at the given column and row. Accessing rows or
   *  columns out of bounds, or request the wrong type is undefined.*/
  int get_int(size_t col, size_t row) {
    assert(columns_[col]->get_type() == 'I' && row < schema_->length());
    IntColumn* column = columns_[col]->as_int();
    return column->get(row);
  }

  bool get_bool(size_t col, size_t row) {
    assert(columns_[col]->get_type() == 'B' && row < schema_->length());
    BoolColumn* column = columns_[col]->as_bool();
    return column->get(row);
  }

  float get_float(size_t col, size_t row) {
    assert(columns_[col]->get_type() == 'F' && row < schema_->length());
    FloatColumn* column = columns_[col]->as_float();
    return column->get(row);
  }

  String* get_string(size_t col, size_t row) {
    assert(columns_[col]->get_type() == 'S' && row < schema_->length());
    StringColumn* column = columns_[col]->as_string();
    return column->get(row);
  }



  /** Set the fields of the given row object with values from the columns at
    * the given offset.  If the row is not from the same schema as the
    * dataframe, results are undefined.
    */
  void fill_row(size_t idx, Row& row) {
    assert(row.col_types == schema_->col_types);

    for (int i = 0; i < schema_->width(); i++) {
      char type = columns_[i]->get_type();
      if (type == 'B') {
        BoolColumn* new_col = dynamic_cast<BoolColumn*>(columns_[i]);
        row.set(i, new_col->get(idx));
      }
      else if (type == 'I') {
        IntColumn* new_col = dynamic_cast<IntColumn*>(columns_[i]);
        row.set(i, new_col->get(idx));
      }
      else if (type == 'F') {
        FloatColumn* new_col = dynamic_cast<FloatColumn*>(columns_[i]);
        row.set(i, new_col->get(idx));
      }
      else if (type == 'S') {
        StringColumn* new_col = dynamic_cast<StringColumn*>(columns_[i]);
        row.set(i, new_col->get(idx));
      }
    }
  }

  /** Add a row at the end of this dataframe. The row is expected to have
   *  the right schema and be filled with values, otherwise undefined.  */
  void add_row(Row& row) {
    assert(row.width() == schema_->width());
    for (int i = 0; i < schema_->width(); i++) {
      char type = columns_[i]->get_type();
      if (type == 'B') {
        columns_[i]->push_back(row.get_bool(i));
      }
      else if (type == 'I') {

        columns_[i]->push_back(row.get_int(i));
      }
      else if (type == 'F') {
        columns_[i]->push_back(row.get_float(i));
      }
      else if (type == 'S') {
        columns_[i]->push_back(row.get_string(i));
      }
    }
    schema_->nrows +=1;
  }

  /** The number of rows in the dataframe. */
  size_t nrows() {
    return schema_->length();
  }

  /** The number of columns in the dataframe.*/
  size_t ncols() {
    return schema_->width();
  }

  /** Visit rows in order */
  void map(Rower& r) {
    for (int i = 0; i < schema_->nrows; i++) {
      Row* current_row = new Row(*schema_);
      for (int j = 0; j < schema_->ncols; j++) {
        char type = columns_[j]->get_type();
        if (type == 'B') {
          BoolColumn* new_col = dynamic_cast<BoolColumn*>(columns_[j]);
          current_row->set(j, new_col->get(i));
        }
        else if (type == 'I') {
          IntColumn* new_col = dynamic_cast<IntColumn*>(columns_[j]);
            current_row->set(j, new_col->get(i));
        }
        else if (type == 'F') {
          FloatColumn* new_col = dynamic_cast<FloatColumn*>(columns_[j]);
            current_row->set(j, new_col->get(i));
        }
        else if (type == 'S') {
          StringColumn* new_col = dynamic_cast<StringColumn*>(columns_[j]);
            current_row->set(j, new_col->get(i));
        }
      }
      r.accept(*current_row);
      delete current_row;
    }
    r.join_delete(&r);
  }

  /** Create a new dataframe, constructed from rows for which the given Rower
    * returned true from its accept method. */
  DataFrame* filter(Rower& r) {
    DataFrame* df = new DataFrame(*schema_);
    for (int i = 0; i < schema_->nrows; i++) {
      Row* current_row = new Row(*schema_);
      for (int j = 0; j < schema_->ncols; j++) {
        char type = columns_[j]->get_type();
        if (type == 'B') {
          BoolColumn* new_col = dynamic_cast<BoolColumn*>(columns_[j]);
            current_row->set(j, new_col->get(i));
        }
        else if (type == 'I') {
          IntColumn* new_col = dynamic_cast<IntColumn*>(columns_[j]);
            current_row->set(j, new_col->get(i));
        }
        else if (type == 'F') {
          FloatColumn* new_col = dynamic_cast<FloatColumn*>(columns_[j]);
            current_row->set(j, new_col->get(i));
        }
        else if (type == 'S') {
          StringColumn* new_col = dynamic_cast<StringColumn*>(columns_[j]);
            current_row->set(j, new_col->get(i));
        }
      }
      if (r.accept(*current_row)) {
        df->add_row(*current_row);
      }
    }
    return df;
  }

  /** Print the dataframe in SoR format to standard output. */
  void print() {
    PrintRower* pr = new PrintRower();
    for (int i = 0; i < schema_->width(); i++) {
        p(i);
        p(" ");
    }
    p("\n");
    map(*pr);
  }

  size_t sizeof_serialized() {
    size_t size = sizeof(size_t) * 2; //schema nrows & ncols

    size += schema_->ncols * sizeof(char) * 2; //schema col types

    for (int i = 0; i < schema_->ncols; ++i)
    {
      char type = columns_[i]->get_type();
          if (type == 'B') {
              BoolColumn* bool_ = dynamic_cast<BoolColumn*>(columns_[i]);

              size += sizeof(size_t);
              for (int j = 0; j < bool_->len_; j++) {
                  size += sizeof(bool_->elements_[i]);
              }
          }
          else if (type == 'I') {
              IntColumn* int_ = dynamic_cast<IntColumn*>(columns_[i]);

              size += sizeof(size_t);
              for (int j = 0; j < int_->len_; j++) {
                  size += sizeof(int_->elements_[i]);
              }
          }
          else if (type == 'F') {
              FloatColumn* float_ = dynamic_cast<FloatColumn*>(columns_[i]);

              size += sizeof(size_t);
              for (int j = 0; j < float_->len_; j++) {
                  size += sizeof(float_->elements_[i]);
              }
          }
          else if (type == 'S') {
              StringColumn* stringColumn_ = dynamic_cast<StringColumn*>(columns_[i]);

              size += sizeof(size_t);
              for (int j = 0; j < stringColumn_->len_; j++) {
                  size += sizeof(size_t);
                  size += stringColumn_->elements_[j]->size_ * sizeof(char) * 2;
              }
          }
    }

    return size;
  }

  /** Serializes this DataFrame into a char*. */
  //TODO: utilize serialization methods found in column.h
  const char* serialize() {
      Serialize* s = new Serialize();

      s->serialize(schema_->nrows);
      s->serialize(schema_->ncols);
      s->serialize(schema_->ncols, schema_->col_types);

      for (size_t i = 0; i < schema_->ncols; i++) {
          char type = columns_[i]->get_type();
          if (type == 'B') {
              BoolColumn* bool_ = dynamic_cast<BoolColumn*>(columns_[i]);

              s->serialize(bool_->len_);
              for (int j = 0; j < bool_->len_; j++) {
                  s->serialize(bool_->elements_[j]);
              }
          }
          else if (type == 'I') {
              IntColumn* int_ = dynamic_cast<IntColumn*>(columns_[i]);
              s->serialize(int_->len_);
              for (int j = 0; j < int_->len_; j++) {
                  s->serialize(int_->elements_[j]);
              }
          }
          else if (type == 'F') {
              FloatColumn* float_ = dynamic_cast<FloatColumn*>(columns_[i]);
              s->serialize(float_->len_);
              for (int j = 0; j < float_->len_; j++) {
                  s->serialize(float_->elements_[j]);
              }
          }
          else if (type == 'S') {
              StringColumn* stringColumn_ = dynamic_cast<StringColumn*>(columns_[i]);
              s->serialize(stringColumn_->len_);

              for (int j = 0; j < stringColumn_->len_; j++) {
                  s->serialize(stringColumn_->elements_[j]->size_);
                  s->serialize(stringColumn_->elements_[j]->size_, stringColumn_->elements_[j]->cstr_);
              }
          }
      }

    return s->buffer;
  }

  /** Constructs a DataFrame from a serialized char*. */
  static DataFrame* deserialize(const char* msg) {
    Schema* s = Schema::deserialize(msg);
    DataFrame* df = new DataFrame(*(new Schema()));

    int bytes_read = 2 * sizeof(size_t) + s->ncols * 2;
    for (size_t i = 0; i < s->ncols; i++) {
        char type = s->col_types[i];

        if (type == 'B') {
            BoolColumn* bool_ = BoolColumn::deserialize(msg + bytes_read);
            bytes_read += bool_->len_ * sizeof(bool) + sizeof(size_t);

            df->add_column(bool_, new String(""));
        }
        else if (type == 'I') {
            IntColumn* int_ = IntColumn::deserialize(msg + bytes_read);
            bytes_read += int_->len_ * sizeof(int) + sizeof(size_t);

            df->add_column(int_, new String(""));
        }
        else if (type == 'F') {
            FloatColumn* float_ = FloatColumn::deserialize(msg + bytes_read);
            bytes_read += float_->len_ * sizeof(float) + sizeof(size_t);

            df->add_column(float_, new String(""));
        }
        else if (type == 'S') {
            StringColumn* string_ = StringColumn::deserialize(msg + bytes_read);

            for (int j = 0; j < string_->len_; ++j)
            {
              bytes_read += string_->elements_[j]->num_bytes();
            }
            bytes_read += sizeof(size_t);

            df->add_column(string_, new String(""));
        }
    }

    return df;
  }

  /** Adds a serialized dataframe to the specified KVS using the specified key.
    *  Returns original DataFrame.
  */
  static DataFrame* add_to_kvs(Key* key, KeyValueStore* kvs, DataFrame* df) {
    // Serialize DF -> value
    const char* serialized_df = df->serialize();

    // Add KV pair into KVS
    Value value(serialized_df, df->sizeof_serialized());
    kvs->put(*key, value);

    return df;
  }

  /** Constructs a DataFrame from an array of boolean values and adds the DataFrame to
   *  the specified KVS using the specified key.
  */
  static DataFrame* fromArray(Key* key, KeyValueStore* kvs, size_t SZ, bool* vals) {

    // Create Dataframe using empty schema
    DataFrame* df = new DataFrame();

    // Create column storing all bool array vals
    BoolColumn* boolColumn = new BoolColumn();
    for (size_t i = 0; i < SZ; i++) {
      boolColumn->push_back(vals[i]);
    }

    // Add values to DF from bool array. Empty string name
    df->add_column(boolColumn, new String(""));

    return DataFrame::add_to_kvs(key, kvs, df);
  }



  /** Constructs a DataFrame from an array of int values and adds the DataFrame to
   *  the specified KVS using the specified key.
  */
  static DataFrame* fromArray(Key* key, KeyValueStore* kvs, size_t SZ, int* vals) {

    // Create Dataframe using empty schema
    DataFrame* df = new DataFrame();

    // Create column storing all bool array vals
    IntColumn* intColumn = new IntColumn();
    for (size_t i = 0; i < SZ; i++) {
      intColumn->push_back(vals[i]);
    }

    // Add values to DF from Int array. Empty string name
    df->add_column(intColumn, new String(""));

    return DataFrame::add_to_kvs(key, kvs, df);
  }

  /** Constructs a DataFrame from an array of float values and adds the DataFrame to
   *  the specified KVS using the specified key.
  */
  static DataFrame* fromArray(Key* key, KeyValueStore* kvs, size_t SZ, float* vals) {

    // Create Dataframe using empty schema
    DataFrame* df = new DataFrame();

    // Create column storing all bool array vals
    FloatColumn* floatColumn = new FloatColumn();
    for (size_t i = 0; i < SZ; i++) {
      floatColumn->push_back(vals[i]);
    }

    // Add values to DF from Float array. Empty string name
    df->add_column(floatColumn, new String(""));

    return DataFrame::add_to_kvs(key, kvs, df);
  }

  /** Constructs a DataFrame from an array of String* values and adds the DataFrame to
   *  the specified KVS using the specified key.
  */
    static DataFrame* fromArray(Key* key, KeyValueStore* kvs, size_t SZ, String** vals) {

      // Create Dataframe using empty schema
      DataFrame* df = new DataFrame();

    // Create column storing all bool array vals
    StringColumn* stringColumn = new StringColumn();
    for (size_t i = 0; i < SZ; i++) {
      stringColumn->push_back(vals[i]);
    }

    // Add values to DF from String array. Empty string name
    df->add_column(stringColumn, new String(""));

    return DataFrame::add_to_kvs(key, kvs, df);
  }

  /** Constructs a DataFrame from a single int value and adds the DataFrame to
   *  the specified KVS using the specified key.
  */
  static DataFrame* fromScalar(Key* key, KeyValueStore* kvs, int sum) {

    // Create Dataframe using empty schema
    DataFrame* df = new DataFrame();

    // Add double value into DF
    IntColumn* intColumn = new IntColumn();
    intColumn->push_back(sum);
    df->add_column(intColumn, new String(""));

    return DataFrame::add_to_kvs(key, kvs, df);
  }

  /** Constructs a DataFrame from a single float value and adds the DataFrame to
   *  the specified KVS using the specified key.
  */
  static DataFrame* fromScalar(Key* key, KeyValueStore* kvs, float sum) {

  // Create Dataframe using empty schema
    DataFrame* df = new DataFrame();

    // Add size_t value into DF
    FloatColumn* floatColumn = new FloatColumn();
    floatColumn->push_back(sum);
    df->add_column(floatColumn, new String(""));
    return DataFrame::add_to_kvs(key, kvs, df);

  }

  /** Constructs a DataFrame from a single bool value and adds the DataFrame to
   *  the specified KVS using the specified key.
  */
  static DataFrame* fromScalar(Key* key, KeyValueStore* kvs, bool b) {

  // Create Dataframe using empty schema
    DataFrame* df = new DataFrame();

    // Add size_t value into DF
    BoolColumn* boolColumn = new BoolColumn();
    boolColumn->push_back(b);
    df->add_column(boolColumn, new String(""));
    return DataFrame::add_to_kvs(key, kvs, df);
  }

  /** Constructs a DataFrame from a single String value and adds the DataFrame to
   *  the specified KVS using the specified key.
  */
  static DataFrame* fromScalar(Key* key, KeyValueStore* kvs, String* str_val) {

  // Create Dataframe using empty schema
    DataFrame* df = new DataFrame();

    // Add size_t value into DF
    StringColumn* stringColumn = new StringColumn();
    stringColumn->push_back(str_val);
    df->add_column(stringColumn, new String(""));
    return DataFrame::add_to_kvs(key, kvs, df);
  }

  // void partition(KeyValueStore* kvs) {
  //
  // }
};




// class Response : public Message {
// public:
//     Value v;
//
//     Response(unsigned sender, unsigned receiver, Value val) {
//         type = 'V';
//         src = sender;
//         dest = receiver;
//         payload = " ";
//         v = val;
//     }
//
//     ~Response() {}
//
//     const char* serialize() {
//         // printf("In RESP Serialize\n");
//         return v.serialize();
//     }
//
//     size_t sizeof_serialized() {
//       //deserialize df and return its size using method upon df (TODO)
//         DataFrame* d = DataFrame::deserialize(v.serialized_data);
//         size_t size = d->sizeof_serialized();
//         return size;
//     }
//
//     static Response* deserialize(const char* msg) {
//         Value* val = Value::deserialize(msg);
//
//         Response* message = new Response(0, 0, *val);
//
//         return message;
//
//     }
// };



// size_t Value::sizeof_serialized() {
//     DataFrame* d = DataFrame::deserialize(serialized_data);
//     size_t size = d->sizeof_serialized();
//     return size;
// }

// void KeyValueStore::run() {
// //         arg_t* arguments = (arg_t*) args;

// //         NetworkIP* this_network = arguments->network_ip;
// //         map<Key, Value, cmp_keys>* this_store = arguments->store;
// // printf("%zu\n", this_network->this_node_);
//         while (1) {
// printf("waiting for msg in THREAD\n");
//             //node 2 deserializes key, gets value from its kv store
//             Message* req = network->recv_m();
// printf("recv req on THREAD\n");
//             Key* k = Key::deserialize(req->payload);
// printf("looking for key named %s on node %zu\n", k->name->c_str(), k->home);
//             Value val = kv->at(*k);
//             //node 2 serializes value and and send msg
//             Message* resp = new Message(network->index(), req->src, 'V', val.serialize(), val.sizeof_serialized());
//             network->send_message(resp);
// printf("sent resp on THREAD\n");
//         }
//     }
