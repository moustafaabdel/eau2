#include <assert.h>
#include "../../src/dataframe/dataframe.h"
#include "../../src/store/store.h"

/***********************************************************
 * DataFrame Tests.
 **********************************************************/

 int main (int argc, char* argv[]) {

   size_t dataframe_tests = 0;

   /** Construct a dataframe with an empty schema. */
   Schema empty_schema("");
   DataFrame first_df(empty_schema);

   Schema& empty_schm = first_df.get_schema();
   assert(empty_schm.nrows == 0);
   assert(empty_schm.ncols == 0);
   dataframe_tests += 2;

   /** Construct a dataframe with a non-empty schema. */
   Schema non_empty("BIFS");
   DataFrame df_non_empty(non_empty);

   Schema& non_empty_schm = df_non_empty.get_schema();
   assert(non_empty_schm.nrows == 0);
   assert(non_empty_schm.ncols == 4);
   dataframe_tests += 2;

   /** Add empty columns to an empty dataframe. Empty column names. */
   BoolColumn* boolColumn = new BoolColumn();
   IntColumn* intColumn = new IntColumn();
   FloatColumn* floatColumn = new FloatColumn();
   StringColumn* stringColumn = new StringColumn();
   String* empty_String = new String("");

   Schema& first_df_schm = first_df.get_schema();

   first_df.add_column(boolColumn, empty_String);
   assert(first_df_schm.ncols == 1);
   assert(first_df_schm.nrows == 0);

   first_df.add_column(intColumn, empty_String);
   assert(first_df_schm.ncols == 2);
   assert(first_df_schm.nrows == 0);

   first_df.add_column(floatColumn, empty_String);
   assert(first_df_schm.ncols == 3);
   assert(first_df_schm.nrows == 0);

   first_df.add_column(stringColumn, empty_String);
   assert(first_df_schm.ncols == 4);
   assert(first_df_schm.nrows == 0);

   dataframe_tests += 8;

   /** Add non-empty columns to an empty dataframe. Empty column names. */
   boolColumn->push_back(false);
   boolColumn->push_back(true);
   boolColumn->push_back(false);
   boolColumn->push_back(true);
   boolColumn->push_back(false);
   boolColumn->push_back(true);
   boolColumn->push_back(false);
   boolColumn->push_back(true);
   boolColumn->push_back(false);

   intColumn->push_back(1);
   intColumn->push_back(3);
   intColumn->push_back(6);
   intColumn->push_back(9);
   intColumn->push_back(12);
   intColumn->push_back(15);
   intColumn->push_back(18);
   intColumn->push_back(21);
   intColumn->push_back(24);

   floatColumn->push_back(1.1f);
   floatColumn->push_back(3.3f);
   floatColumn->push_back(6.6f);
   floatColumn->push_back(9.9f);
   floatColumn->push_back(12.12f);
   floatColumn->push_back(15.15f);
   floatColumn->push_back(18.18f);
   floatColumn->push_back(21.21f);
   floatColumn->push_back(24.24f);

   stringColumn->push_back(new String("First"));
   stringColumn->push_back(new String("Second"));
   stringColumn->push_back(new String("3"));
   stringColumn->push_back(new String("Fourth"));
   stringColumn->push_back(new String("Fifth"));
   stringColumn->push_back(new String("6th"));
   stringColumn->push_back(new String("7th"));
   stringColumn->push_back(new String("Eighth"));
   stringColumn->push_back(new String("Ninth 9 value hello whats good its ya boy"));

   Schema second_empty_schema("");
   DataFrame second_df(second_empty_schema);
   Schema& second_df_schm = second_df.get_schema();

   second_df.add_column(boolColumn, empty_String);
   assert(second_df_schm.ncols == 1);
   assert(second_df_schm.nrows == 9);

   second_df.add_column(intColumn, empty_String);
   assert(second_df_schm.ncols == 2);
   assert(second_df_schm.nrows == 9);

   second_df.add_column(floatColumn, empty_String);
   assert(second_df_schm.ncols == 3);
   assert(second_df_schm.nrows == 9);

   second_df.add_column(stringColumn, empty_String);
   assert(second_df_schm.ncols == 4);
   assert(second_df_schm.nrows == 9);

   dataframe_tests += 8;

   /** Assert values were placed in the proper positions. */
   assert(second_df.get_bool(0,5) == boolColumn->elements_[5]);
   assert(second_df.get_int(1,5) == intColumn->elements_[5]);
   assert(abs(second_df.get_float(2,5)-floatColumn->elements_[5]) < 0.0001);
   assert(second_df.get_string(3,5)->equals(stringColumn->elements_[5]));

   dataframe_tests += 4;

   /** TODO: Not testing get_col and get_row since we did not implement naming rows and columns. */




   /** Add row to the end of a DataFrame. */
   Row row_non_empty(non_empty);
   row_non_empty.set(0, false);
   row_non_empty.set(1, 100000);
   row_non_empty.set(2, 1717.17f);
   row_non_empty.set(3, new String("Added Row"));

   second_df.add_row(row_non_empty);

   assert(second_df_schm.nrows == 10);
   assert(!second_df.get_bool(0,9));
   assert(second_df.get_int(1,9) == 100000);
   assert(abs(second_df.get_float(2,9)-1717.17) < 0.0001);
   assert(second_df.get_string(3,9)->equals(new String("Added Row")));

   dataframe_tests += 5;


   /** Serialization Tests shown in serial_test.cpp. */

   /** Constructing a dataframe from bool array. */
   KeyValueStore* kvs = new KeyValueStore();
   Key* bool_key = new Key("FromBoolArray", 0);


   bool* bool_vals = new bool[5];
   bool_vals[0] = false;
   bool_vals[1] = true;
   bool_vals[2] = false;
   bool_vals[3] = true;
   bool_vals[4] = false;


   DataFrame* bool_arr = DataFrame::fromArray(bool_key, kvs, 5, bool_vals);

   Schema& bool_arr_schema = bool_arr->get_schema();
   assert(bool_arr_schema.ncols == 1);
   assert(bool_arr_schema.nrows == 5);
   assert(!bool_arr->get_bool(0, 2));
   dataframe_tests += 3;


   /** Constructing a dataframe from int array. */
   Key* int_key = new Key("FromIntArray", 0);

   int* int_vals = new int[5];
   int_vals[0] = 1;
   int_vals[1] = 3;
   int_vals[2] = 6;
   int_vals[3] = 9;
   int_vals[4] = 12;

   DataFrame* int_arr = DataFrame::fromArray(int_key, kvs, 5, int_vals);
   Schema& int_arr_schema = int_arr->get_schema();
   assert(int_arr_schema.ncols == 1);
   assert(int_arr_schema.nrows == 5);
   assert(int_arr->get_int(0, 2) == 6);
   dataframe_tests += 3;

   /** Constructing a dataframe from float array. */
   Key* float_key = new Key("FromFloatArray", 0);

   float* float_vals = new float[5];
   float_vals[0] = 1.1;
   float_vals[1] = 3.3;
   float_vals[2] = 5.5;
   float_vals[3] = 7.7;
   float_vals[4] = 9.9;

   DataFrame* float_arr = DataFrame::fromArray(float_key, kvs, 5, float_vals);
   Schema& float_arr_schema = float_arr->get_schema();
   assert(float_arr_schema.ncols == 1);
   assert(float_arr_schema.nrows == 5);
   assert(abs(float_arr->get_float(0, 2) - 5.5) < 0.0001);
   dataframe_tests += 3;

   /** Constructing a dataframe from String array. */
   Key* string_key = new Key("FromStringArray", 0);

   String** string_vals = new String*[5];
   string_vals[0] = new String("Hello");
   string_vals[1] = new String("Second");
   string_vals[2] = new String("Whats :) Good");
   string_vals[3] = new String("Nothing much :/");
   string_vals[4] = new String("This quarantine sucks!!");

   DataFrame* string_arr = DataFrame::fromArray(string_key, kvs, 5, string_vals);
   Schema& string_arr_schema = string_arr->get_schema();
   assert(string_arr_schema.ncols == 1);
   assert(string_arr_schema.nrows == 5);
   assert(string_arr->get_string(0, 2)->equals(new String("Whats :) Good")));
   dataframe_tests += 3;


   /** Constructing a dataframe from float Scalar. */
   Key* scalar_float_key = new Key("FromScalarFloat", 0);

   float float_val = 1.23f;

   DataFrame* scalar_float = DataFrame::fromScalar(scalar_float_key, kvs, float_val);
   Schema& scalar_float_schema = scalar_float->get_schema();
   assert(scalar_float_schema.ncols == 1);
   assert(scalar_float_schema.nrows == 1);
   assert(abs(scalar_float->get_float(0, 0) - 1.23) < 0.0001);
   dataframe_tests += 3;

   /** Constructing a dataframe from int Scalar. */
   Key* scalar_int_key = new Key("FromScalarInt", 0);

   int int_val = 17;

   DataFrame* scalar_int = DataFrame::fromScalar(scalar_int_key, kvs, int_val);
   Schema& scalar_int_schema = scalar_int->get_schema();
   assert(scalar_int_schema.ncols == 1);
   assert(scalar_int_schema.nrows == 1);
   assert(scalar_int->get_int(0, 0) == 17);
   dataframe_tests += 3;

   /** Constructing a dataframe from bool Scalar. */
   Key* scalar_bool_key = new Key("FromScalarBool", 0);

   bool bool_val = true;

   DataFrame* scalar_bool = DataFrame::fromScalar(scalar_bool_key, kvs, bool_val);
   Schema& scalar_bool_schema = scalar_bool->get_schema();
   assert(scalar_bool_schema.ncols == 1);
   assert(scalar_bool_schema.nrows == 1);
   assert(scalar_bool->get_bool(0, 0));
   dataframe_tests += 3;


   /** Constructing a dataframe from bool Scalar. */
   Key* scalar_string_key = new Key("FromScalarString", 0);

   String* string_val = new String("stringKey");

   DataFrame* scalar_string = DataFrame::fromScalar(scalar_string_key, kvs, string_val);
   Schema& scalar_string_schema = scalar_string->get_schema();
   assert(scalar_string_schema.ncols == 1);
   assert(scalar_string_schema.nrows == 1);
   assert(scalar_string->get_string(0, 0)->equals(new String("stringKey")));
   dataframe_tests += 3;

   /** Constructing a dataframe from existing DataFrame. */
   Schema s("IFSBIFSB");

   DataFrame df(s);

   Row  r(df.get_schema());

   for(size_t i = 0; i < 20; i++) {
       r.set(0,(int)(-17*i));
       r.set(1,0.0f);
       r.set(2,new String("SWDEV"));
       r.set(3,false);
       r.set(4,(int)(17*i));
       r.set(5,-10.3f);
       r.set(6,new String("******"));
       r.set(7,true);

       df.add_row(r);
   }

   assert(!df.get_bool(3, 0));
   assert(df.get_bool(7, 0));
   assert(df.get_int(0, 0) == 0);
   assert(df.get_int(0, 1) == -17);
   assert(df.get_string(2, 0)->size() == 5);
   assert(df.get_string(6, 17)->size() == 6);
   assert(abs(df.get_float(1, 5)) < 0.0001);
   assert(abs(df.get_float(5, 1) - -10.3f) < 0.0001);

   assert(df.schema_->col_types[2] == 'S');

   DataFrame* df2 = new DataFrame(df);

   for (int i = 0;  i < df.ncols(); ++ i) {
       assert(df2->schema_->col_types[i] == df.schema_->col_types[i]);
   }

   dataframe_tests += 1;

   printf("\nDataFrame: All %zu dataframe tests passed.\n\n", dataframe_tests);

}
