#include <assert.h>
#include <math.h>
#include "../../src/dataframe/column.h"
#include "../../src/dataframe/schema.h"
#include "../../src/dataframe/dataframe.h"
#include "../../src/util/serializer.h"


/***********************************************************
 * Serialization Tests.
 **********************************************************/

int main (int argc, char* argv[]) {

  /***********************************************************
   * Primitive types:: testing serialization of primitives.
   **********************************************************/
      size_t primitive_tests = 0;

      /**Primitive Types return vals*/
      bool bool_result = true;
      int int_result = 12;
      float float_result = 41.24f;
      size_t size_result = 19;

      /**Serialize Primitive Types*/
      Serialize* first = new Serialize();
      const char* serialized_bool = first->serialize(bool_result);

      Serialize* second = new Serialize();
      const char* serialized_int = second->serialize(int_result);

      Serialize* third = new Serialize();
      const char* serialized_float = third->serialize(float_result);

      Serialize* fourth = new Serialize();
      const char* serialized_size_t = fourth->serialize(size_result);


      /**Deserialize Primitive Types*/
      bool b = Deserialize::deserialize_bool(serialized_bool);
      int i = Deserialize::deserialize_int(serialized_int);
      float f = Deserialize::deserialize_float(serialized_float);
      size_t size = Deserialize::deserialize_size_t(serialized_size_t);

      /** Assert values were correctly serialized & deserialized. */
      assert(b == bool_result);
      assert(i == int_result);
      assert(abs(f-float_result) < 0.0001);
      assert(size == size_result);
      primitive_tests += 4;
      //printf("Primitives: All %zu serialization tests passed.\n", primitive_tests);


      /***********************************************************
       * String:: testing serialization of Strings.
       **********************************************************/
       size_t string_tests = 0;

     /** Strings to be serialized. */
      String* test = new String("Test");
      String* one = new String("one");
      String* two = new String("2");
      String* three = new String("Three Musketeers.");
      String* four = new String("Just another 4 test what it do baby >=) !!.");


      /**Serialize Strings. */
      const char* serialized_strtest = test->serialize();
      const char* serialized_one = one->serialize();
      const char* serialized_two = two->serialize();
      const char* serialized_three = three->serialize();
      const char* serialized_four = four->serialize();


      /** Deserialize Strings. */
      String* test_result = String::deserialize(serialized_strtest);
      String* one_result = String::deserialize(serialized_one);
      String* two_result = String::deserialize(serialized_two);
      String* three_result = String::deserialize(serialized_three);
      String* four_result = String::deserialize(serialized_four);

      /** Assert values were correctly serialized & deserialized. */
      assert(test->equals(test_result));
      assert(one->equals(one_result));
      assert(two->equals(two_result));
      assert(three->equals(three_result));
      assert(four->equals(four_result));
      string_tests += 5;

      //printf("String: All %zu serialization tests passed.\n", string_tests);

  /***********************************************************
   * BoolColumn tests :: testing serialization of BoolColumn.
   **********************************************************/

    size_t bool_serial_tests_passed = 0;

    /** Add all preliminary values to BoolColumn. */
    BoolColumn* boolColumn = new BoolColumn();
    boolColumn->push_back(false);
    boolColumn->push_back(true);
    boolColumn->push_back(false);
    boolColumn->push_back(true);
    boolColumn->push_back(false);
    boolColumn->push_back(true);
    boolColumn->push_back(false);
    boolColumn->push_back(true);
    boolColumn->push_back(false);

    /** Serialize boolColumn and deserialize using serialized msg. */
    const char* serialized_bool_col = boolColumn->serialize();
    BoolColumn* bool_newCol = BoolColumn::deserialize(serialized_bool_col);

    /** Assert values have been correctly serialized and deserialized. */
    assert(bool_newCol->len_ == 9);
    assert(!bool_newCol->elements_[0]);
    assert(bool_newCol->elements_[1]);
    assert(!bool_newCol->elements_[2]);
    assert(bool_newCol->elements_[3]);
    assert(!bool_newCol->elements_[4]);
    assert(bool_newCol->elements_[5]);
    assert(!bool_newCol->elements_[6]);
    assert(bool_newCol->elements_[7]);
    assert(!bool_newCol->elements_[8]);

    assert(bool_newCol->elements_[6] == boolColumn->elements_[6]);
    bool_serial_tests_passed += 11;

    //printf("BoolColumn: All %zu serialization tests passed.\n", bool_serial_tests_passed);


    /***********************************************************
     * IntColumn tests :: testing serialization of IntColumn.
     **********************************************************/
    size_t int_serial_tests_passed = 0;

    /** Add all preliminary values to IntColumn. */
    IntColumn* intColumn = new IntColumn();
    intColumn->push_back(1);
    intColumn->push_back(3);
    intColumn->push_back(6);
    intColumn->push_back(9);
    intColumn->push_back(12);
    intColumn->push_back(15);
    intColumn->push_back(18);
    intColumn->push_back(21);
    intColumn->push_back(24);

    /** Serialize intColumn and deserialize using serialized msg. */
    const char* serialized_int_col = intColumn->serialize();
    IntColumn* int_newCol = IntColumn::deserialize(serialized_int_col);

    /** Assert values have been correctly serialized and deserialized. */
    assert(int_newCol->len_ == 9);
    assert(int_newCol->elements_[0] == 1);
    assert(int_newCol->elements_[1] == 3);
    assert(int_newCol->elements_[2] == 6);
    assert(int_newCol->elements_[3] == 9);
    assert(int_newCol->elements_[4] == 12);
    assert(int_newCol->elements_[5] == 15);
    assert(int_newCol->elements_[6] == 18);
    assert(int_newCol->elements_[7] == 21);
    assert(int_newCol->elements_[8] == 24);
    assert(int_newCol->elements_[6] == intColumn->elements_[6]);
    int_serial_tests_passed += 11;

    //printf("IntColumn: All %zu serialization tests passed.\n", int_serial_tests_passed);


    /***********************************************************
     * FloatColumn tests :: testing serialization of FloatColumn.
     **********************************************************/
     size_t float_serial_tests_passed = 0;

     /** Add all preliminary values to FloatColumn. */
    FloatColumn* floatColumn = new FloatColumn();
    floatColumn->push_back(1.1f);
    floatColumn->push_back(3.3f);
    floatColumn->push_back(6.6f);
    floatColumn->push_back(9.9f);
    floatColumn->push_back(12.12f);
    floatColumn->push_back(15.15f);
    floatColumn->push_back(18.18f);
    floatColumn->push_back(21.21f);
    floatColumn->push_back(24.24f);

    /** Serialize floatColumn and deserialize using serialized msg. */
    const char* serialized_float_col = floatColumn->serialize();
    FloatColumn* float_newCol = FloatColumn::deserialize(serialized_float_col);

    /** Assert values have been correctly serialized and deserialized. */
    assert(float_newCol->len_ == 9);
    assert(abs(float_newCol->elements_[0] - 1.1f) < 0.0001);
    assert(abs(float_newCol->elements_[1] - 3.3f) < 0.0001);
    assert(abs(float_newCol->elements_[2] - 6.6f) < 0.0001);
    assert(abs(float_newCol->elements_[3] - 9.9f) < 0.0001);
    assert(abs(float_newCol->elements_[4] - 12.12f) < 0.0001);
    assert(abs(float_newCol->elements_[5] - 15.15f) < 0.0001);
    assert(abs(float_newCol->elements_[6] - 18.18f) < 0.0001);
    assert(abs(float_newCol->elements_[7] - 21.21f) < 0.0001);
    assert(abs(float_newCol->elements_[8] - 24.24f) < 0.0001);
    assert(abs(float_newCol->elements_[6] - floatColumn->elements_[6]) < 0.0001);
    float_serial_tests_passed += 11;

    //printf("FloatColumn: All %zu serialization tests passed.\n", float_serial_tests_passed);


    /***********************************************************
     * StringColumn tests :: testing serialization of StringColumn.
     **********************************************************/
     size_t string_serial_tests_passed = 0;

     /** Add all preliminary values to StringColumn. */
    StringColumn* strColumn = new StringColumn();
    strColumn->push_back(new String("First"));
    strColumn->push_back(new String("Second"));
    strColumn->push_back(new String("3"));
    strColumn->push_back(new String("Fourth"));
    strColumn->push_back(new String("Fifth"));
    strColumn->push_back(new String("6th"));
    strColumn->push_back(new String("7th"));
    strColumn->push_back(new String("Eighth"));
    strColumn->push_back(new String("Ninth 9 value hello whats good its ya boy"));

    /** Serialize stringColumn and deserialize using serialized msg. */
    const char* serialized_string_column = strColumn->serialize();
    StringColumn* string_newCol = StringColumn::deserialize(serialized_string_column);

    /** Assert values have been correctly serialized and deserialized. */
    assert(string_newCol->len_ == 9);
    assert(string_newCol->elements_[0]->equals(strColumn->elements_[0]));
    assert(string_newCol->elements_[1]->equals(strColumn->elements_[1]));
    assert(string_newCol->elements_[2]->equals(strColumn->elements_[2]));
    assert(string_newCol->elements_[3]->equals(strColumn->elements_[3]));
    assert(string_newCol->elements_[4]->equals(strColumn->elements_[4]));
    assert(string_newCol->elements_[5]->equals(strColumn->elements_[5]));
    assert(string_newCol->elements_[6]->equals(strColumn->elements_[6]));
    assert(string_newCol->elements_[7]->equals(strColumn->elements_[7]));
    assert(string_newCol->elements_[8]->equals(strColumn->elements_[8]));
    assert(string_newCol->elements_[6]->equals(strColumn->elements_[6]));
    string_serial_tests_passed += 11;

    //printf("StringColumn: All %zu serialization tests passed.\n", string_serial_tests_passed);


    /***********************************************************
     * DataFrame tests :: testing serialization of DataFrame.
     **********************************************************/
     size_t dataFrame_serial_tests_passed = 0;

     /** Prepare columns to be added. */
    Column* test_boolDF = new BoolColumn();
    test_boolDF->push_back(false);
    test_boolDF->push_back(true);
    test_boolDF->push_back(false);
    test_boolDF->push_back(true);
    test_boolDF->push_back(false);
    test_boolDF->push_back(true);
    test_boolDF->push_back(false);
    test_boolDF->push_back(true);
    test_boolDF->push_back(false);

    IntColumn* test_intDF = new IntColumn();
    test_intDF->push_back(1);
    test_intDF->push_back(3);
    test_intDF->push_back(6);
    test_intDF->push_back(9);
    test_intDF->push_back(12);
    test_intDF->push_back(15);
    test_intDF->push_back(18);
    test_intDF->push_back(21);
    test_intDF->push_back(24);

    FloatColumn* test_floatDF = new FloatColumn();
    test_floatDF->push_back(1.1f);
    test_floatDF->push_back(3.3f);
    test_floatDF->push_back(6.6f);
    test_floatDF->push_back(9.9f);
    test_floatDF->push_back(12.12f);
    test_floatDF->push_back(15.15f);
    test_floatDF->push_back(18.18f);
    test_floatDF->push_back(21.21f);
    test_floatDF->push_back(24.24f);

    StringColumn* test_stringDF = new StringColumn();
    test_stringDF->push_back(new String("First"));
    test_stringDF->push_back(new String("Second"));
    test_stringDF->push_back(new String("3"));
    test_stringDF->push_back(new String("Fourth"));
    test_stringDF->push_back(new String("Fifth"));
    test_stringDF->push_back(new String("6th"));
    test_stringDF->push_back(new String("7th"));
    test_stringDF->push_back(new String("Eighth"));
    test_stringDF->push_back(new String("Ninth 9 value hello whats good its ya boy"));

    /** Initialize Dataframe with 2 Int Columns holding int values. */
    Schema s("II");
    DataFrame df(s);
    Row r(df.get_schema());
    for(size_t i = 0; i <  9; i++) {
        r.set(0,(int)i);
        r.set(1,(int)i+1);
        df.add_row(r);
    }

    /** Add columns. */
    df.add_column(test_boolDF, new String("bool"));
    df.add_column(test_intDF, new String("int"));
    df.add_column(test_floatDF, new String(""));
    df.add_column(test_stringDF, new String(""));

    /** Serialize and deserialize DataFrame. */
    const char* serialized_df = df.serialize();
    DataFrame* df_deserial = DataFrame::deserialize(serialized_df);

    /** Assert values have been correctly serialized and deserialized. */
    assert(df.nrows() == df_deserial->nrows());
    assert(df.ncols() == df_deserial->ncols());
    assert(df.get_int(0,0) == df_deserial->get_int(0,0));
    assert(df_deserial->get_string(5,1)->equals(new String("Second")));
    dataFrame_serial_tests_passed += 4;

    //printf("DataFrame: All %zu serialization tests passed.\n", dataFrame_serial_tests_passed);

    /***********************************************************
     * Serialization tests :: All Serialization Tests.
     **********************************************************/
    size_t all_tests = primitive_tests + string_tests + bool_serial_tests_passed + int_serial_tests_passed
    + float_serial_tests_passed + string_serial_tests_passed + dataFrame_serial_tests_passed;
    

    Message* msg = new Message(3, 6, 'K', serialized_df, df.sizeof_serialized());
    const char* serial_msg = msg->serialize();
    Message* deserial_msg = Message::deserialize(serial_msg);

    // printf("%c\n", deserial_msg->type);
    // printf("%u\n", deserial_msg->src);
    // printf("%u\n", deserial_msg->dest);
    // printf("%s\n", deserial_msg->payload);

    assert(deserial_msg->type == 'K');
    assert(deserial_msg->src == 3);
    assert(deserial_msg->dest == 6);
    assert(strcmp(serialized_df, deserial_msg->payload) == 0);

    printf("\nSerialization: All %zu serialization tests passed.\n\n", all_tests);

    return 0;
};
