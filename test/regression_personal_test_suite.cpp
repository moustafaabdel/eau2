#include <math.h>
#include <iostream>
#include "../src/dataframe/dataframe.h"


void FAIL() {   exit(1);    }
void OK(const char* m) { printf("%s\n", m); }
void t_true(bool p) { if (!p) FAIL(); }
void t_false(bool p) { if (p) FAIL(); }

void test() {
  Schema s("FI");

  DataFrame df(s);
  Row r(df.get_schema());
  for(size_t i = 0; i < 1000; i++) {
      r.set(0,(float)(i+0.7));
      r.set(1,(int)i+1);
      df.add_row(r);
  }
  OK("TEST PASSED");
}

//Column Test
void test_column() {
    BoolColumn* boolColumn = new BoolColumn();
    IntColumn* intColumn = new IntColumn();
    FloatColumn* floatColumn = new FloatColumn();
    StringColumn* stringColumn = new StringColumn();

    //Ensure columns are created with initial size of 0
    t_true(boolColumn->size() == 0);
    t_true(intColumn->size() == 0);
    t_true(floatColumn->size() == 0);
    t_true(stringColumn->size() == 0);

    //Ensure columns return nullptrs when invoked incorrectly
    t_true(boolColumn->as_float() == nullptr);
    t_true(boolColumn->as_int() == nullptr);
    t_true(boolColumn->as_string() == nullptr);

    t_true(intColumn->as_bool() == nullptr);
    t_true(intColumn->as_float() == nullptr);
    t_true(intColumn->as_string() == nullptr);

    t_true(floatColumn->as_bool() == nullptr);
    t_true(floatColumn->as_int() == nullptr);
    t_true(floatColumn->as_string() == nullptr);

    t_true(stringColumn->as_bool() == nullptr);
    t_true(stringColumn->as_int() == nullptr);
    t_true(stringColumn->as_float() == nullptr);

    //Add and check elements added to a bool column
    boolColumn->push_back(false);
    t_true(boolColumn->size() == 1);
    t_true(boolColumn->get(0) == false);

    //Add and check elements added to an int column
    intColumn->push_back(18);
    intColumn->push_back(true);
    intColumn->push_back(7);
    t_true(intColumn->size() == 3);
    t_true(intColumn->get(2) == 7);

    //Add and check elements added to a float column
    floatColumn->push_back(18.9f);
    floatColumn->push_back(7.4f);
    floatColumn->push_back(true);
    t_true(floatColumn->size() == 3);
    t_true(abs(floatColumn->get(1)-7.4f) < 0.0001);

    //Add and check elements added to a string column
    String* test = new String("hello");
    stringColumn->push_back(test);
    t_true(stringColumn->size() == 1);
    t_true(stringColumn->get(0)->equals(test));

    Column* col = boolColumn;
    t_true(col->get_type() == 'B');

    OK("Column Tested");
}


void test_row() {
    Schema* schema = new Schema("BIFS");

    //Create row using existing schema
    Row* row = new Row(*schema);

    //Set values
    row->set(0, false);
    row->set(1, 7);
    row->set(2, -18.8f);
    row->set(3, new String("String Test"));

    //Ensure updated elements are correctly reflected in the row
    t_true(row->get_bool(0) == false);
    t_true(row->get_int(1) == 7);
    t_true(abs(row->get_float(2)+18.8f) < 0.0001f);
    t_true(row->width() == 4);

    OK("Row Tested");
}


void test_print_df() {
    Schema s("IFSBIFSB");

    DataFrame df(s);
    Row  r(df.get_schema());
    for(size_t i = 0; i < 20; i++) {
        r.set(0,(int)(-17*i));
        r.set(1,0.0f);
        r.set(2,new String("SWDEV"));
        r.set(3,1);
        r.set(4,(int)(17*i));
        r.set(5,-10.3f);
        r.set(6,new String("******"));
        r.set(7,0);

        df.add_row(r);
    }

    df.print();
    OK("Printed Dataframe");
}


void fill_row() {
    Schema s("II");

    DataFrame df(s);
    Row r(df.get_schema());
    for(size_t i = 0; i <  2; i++) {
        r.set(0,(int)i);
        r.set(1,(int)i+1);
        df.add_row(r);
    }
    Row* row = new Row(s);
    df.fill_row(0, *row);

    df.add_row(*row);
    t_true(df.nrows() == 3);
    t_true(df.get_int(0,2) == 0);

    OK("Fill row regression test passed.");
}

void basic_example() {
    Schema s("II");

    DataFrame df(s);
    Row r(df.get_schema());
    for(size_t i = 0; i <  1000 * 100; i++) {
        r.set(0,(int)i);
        r.set(1,(int)i+1);
        df.add_row(r);
    }
    t_true(df.get_int((size_t)0,1) == 1);

    OK("Basic example passed");
}

int main(int argc, char **argv) {
    test();
    test_column();
    test_row();
    //test_print_df();
    fill_row();
    basic_example();

    return 0;
}
