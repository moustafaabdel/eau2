#include <assert.h>
#include "../../src/dataframe/column.h"
#include "../../src/util/string.h"


/***********************************************************
 * Column Tests.
 **********************************************************/

int main (int argc, char* argv[]) {

  /***********************************************************
   * BoolColumn tests :: testing functionality of BoolColumn.
   **********************************************************/

  size_t bool_tests_passed = 0;
  BoolColumn* boolColumn = new BoolColumn();
  /** Starting size of 0. */
  assert(boolColumn->len_ == 0);
  bool_tests_passed += 1;

  /** Adding correct type will update length and array of values. */
  boolColumn->push_back(false);
  assert(boolColumn->len_ == 1);
  assert(!boolColumn->elements_[0]);
  bool_tests_passed += 2;


  boolColumn->push_back(false);
  assert(boolColumn->len_ == 2);
  bool_tests_passed += 1;

  /** Type converter: will only return the same column under its actual bool type. */
  assert(boolColumn->as_int() == nullptr);
  bool_tests_passed += 1;

  assert(boolColumn->as_float() == nullptr);
  bool_tests_passed += 1;

  assert(boolColumn->as_string() == nullptr);
  bool_tests_passed += 1;

  BoolColumn* bool_typeConverter = boolColumn->as_bool();
  assert(bool_typeConverter->len_ == boolColumn->len_);
  assert(bool_typeConverter->size_ == boolColumn->size_);
  assert(bool_typeConverter->elements_[0] == boolColumn->elements_[0]);
  assert(bool_typeConverter->elements_[1] == boolColumn->elements_[1]);
  bool_tests_passed += 4;


  /** Serializing and deserializing will correctly construct a BoolColumn
   holding the same values */
   const char* serialized_bool_col = boolColumn->serialize();
   BoolColumn* deserialized_bool_col = BoolColumn::deserialize(serialized_bool_col);
   assert(deserialized_bool_col->len_ == boolColumn->len_);
   assert(deserialized_bool_col->size_ == boolColumn->size_);
   assert(deserialized_bool_col->elements_[0] == boolColumn->elements_[0]);
   assert(deserialized_bool_col->elements_[1] == boolColumn->elements_[1]);
   bool_tests_passed += 4;

   //printf("BoolColumn: All %zu tests passed.\n", bool_tests_passed);

   /***********************************************************
    * IntColumn tests :: testing functionality of IntColumn.
    **********************************************************/

   size_t int_tests_passed = 0;
   IntColumn* intColumn = new IntColumn();
   /** Starting size of 0. */
   assert(intColumn->len_ == 0);
   int_tests_passed += 1;

   /** Adding correct type will update length and array of values. */
   intColumn->push_back(false);
   assert(intColumn->len_ == 1);
   assert(intColumn->elements_[0] == 0);
   int_tests_passed += 2;

   intColumn->push_back(17);
   assert(intColumn->len_ == 2);
   assert(intColumn->elements_[1] == 17);
   int_tests_passed += 2;


   /** Type converter: will only return the same column under its actual int type. */
   assert(intColumn->as_bool() == nullptr);
   int_tests_passed += 1;

   assert(intColumn->as_float() == nullptr);
   int_tests_passed += 1;

   assert(intColumn->as_string() == nullptr);
   int_tests_passed += 1;

   IntColumn* int_typeConverter = intColumn->as_int();
   assert(int_typeConverter->len_ == intColumn->len_);
   assert(int_typeConverter->size_ == intColumn->size_);
   assert(int_typeConverter->elements_[0] == intColumn->elements_[0]);
   assert(int_typeConverter->elements_[1] == intColumn->elements_[1]);
   int_tests_passed += 4;


   /** Serializing and deserializing will correctly construct an IntColumn
    holding the same values */
    const char* serialized_int_col = intColumn->serialize();
    IntColumn* deserialized_int_col = IntColumn::deserialize(serialized_int_col);
    assert(deserialized_int_col->len_ == intColumn->len_);
    assert(deserialized_int_col->size_ == intColumn->size_);
    assert(deserialized_int_col->elements_[0] == intColumn->elements_[0]);
    assert(deserialized_int_col->elements_[1] == intColumn->elements_[1]);
    int_tests_passed += 4;

    //printf("IntColumn: All %zu tests passed.\n", int_tests_passed);


    /***********************************************************
     * FloatColumn tests :: testing functionality of FloatColumn.
     **********************************************************/

    size_t float_tests_passed = 0;
    FloatColumn* floatColumn = new FloatColumn();
    /** Starting size of 0. */
    assert(floatColumn->len_ == 0);
    float_tests_passed += 1;

    /** Adding correct type will update length and array of values. */
    floatColumn->push_back(1.1f);
    assert(floatColumn->len_ == 1);
    float_tests_passed += 1;

    floatColumn->push_back(true);
    assert(floatColumn->len_ == 2);
    assert(abs(floatColumn->elements_[1] - 1) < 0.0001);
    float_tests_passed += 2;

    floatColumn->push_back(17);
    assert(floatColumn->len_ == 3);
     assert(abs(floatColumn->elements_[2] - 17.0) < 0.0001);
    float_tests_passed += 2;


    /** Type converter: will only return the same column under its actual float type. */
    assert(floatColumn->as_bool() == nullptr);
    float_tests_passed += 1;

    assert(floatColumn->as_int() == nullptr);
    float_tests_passed += 1;

    assert(floatColumn->as_string() == nullptr);
    float_tests_passed += 1;

    FloatColumn* float_typeConverter = floatColumn->as_float();
    assert(float_typeConverter->len_ == floatColumn->len_);
    assert(float_typeConverter->size_ == floatColumn->size_);
    assert(abs(float_typeConverter->elements_[0] - floatColumn->elements_[0]) < 0.0001);
    assert(abs(float_typeConverter->elements_[1] - floatColumn->elements_[1]) < 0.0001);
    float_tests_passed += 4;


    /** Serializing and deserializing will correctly construct a FloatColumn
     holding the same values */
     const char* serialized_float_col = floatColumn->serialize();
     FloatColumn* deserialized_float_col = FloatColumn::deserialize(serialized_float_col);
     assert(deserialized_float_col->len_ == floatColumn->len_);
     assert(deserialized_float_col->size_ == floatColumn->size_);
     assert(abs(deserialized_float_col->elements_[0] - floatColumn->elements_[0]) < 0.0001);
     assert(abs(deserialized_float_col->elements_[1] - floatColumn->elements_[1]) < 0.0001);
     float_tests_passed += 4;

     //printf("FloatColumn: All %zu tests passed.\n", float_tests_passed);

     /***********************************************************
      * StringColumn tests :: testing functionality of StringColumn.
      **********************************************************/

     size_t string_tests_passed = 0;
     StringColumn* stringColumn = new StringColumn();
     /** Starting size of 0. */
     assert(stringColumn->len_ == 0);
     string_tests_passed += 1;


     /** Adding correct type will update length and array of values. */
     stringColumn->push_back(new String("String Test"));
     assert(stringColumn->len_ == 1);
     assert(stringColumn->elements_[0]->equals(new String("String Test")));
     string_tests_passed += 2;


     stringColumn->push_back(new String("Hello"));
     assert(stringColumn->len_ == 2);
     string_tests_passed += 1;

     /** Type converter: will only return the same column under its actual String type. */
     assert(stringColumn->as_int() == nullptr);
     string_tests_passed += 1;

     assert(stringColumn->as_float() == nullptr);
     string_tests_passed += 1;

     assert(stringColumn->as_bool() == nullptr);
     string_tests_passed += 1;

     StringColumn* string_typeConverter = stringColumn->as_string();
     assert(string_typeConverter->len_ == stringColumn->len_);
     assert(string_typeConverter->size_ == stringColumn->size_);
     assert(string_typeConverter->elements_[0]->equals(stringColumn->elements_[0]));
     assert(string_typeConverter->elements_[1]->equals(stringColumn->elements_[1]));
     string_tests_passed += 4;


     /** Serializing and deserializing will correctly construct a StringColumn
      holding the same values */
      const char* serialized_string_col = stringColumn->serialize();
      StringColumn* deserialized_string_col = StringColumn::deserialize(serialized_string_col);
      assert(deserialized_string_col->len_ == stringColumn->len_);
      assert(deserialized_string_col->size_ == stringColumn->size_);
      assert(deserialized_string_col->elements_[0]->equals(stringColumn->elements_[0]));
      assert(deserialized_string_col->elements_[1]->equals(stringColumn->elements_[1]));
      string_tests_passed += 4;

      //printf("BoolColumn: All %zu tests passed.\n", string_tests_passed);


      /***********************************************************
       * All Column tests.
       **********************************************************/
       size_t all_columns = bool_tests_passed + int_tests_passed + float_tests_passed + string_tests_passed;
       printf("\nColumns: All %zu Column tests passed.\n\n", all_columns);

}
