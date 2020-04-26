#include <assert.h>
#include "../../src/dataframe/row.h"

/***********************************************************
 * Row Tests.
 **********************************************************/

 int main (int argc, char* argv[]) {

   size_t row_tests = 0;

   Schema non_empty("BIFS");

   /** Add elements to a row using a given schema. */
   Row row(non_empty);
   row.set(0, false);
   row.set(1, 100000);
   row.set(2, 1717.17f);
   row.set(3, new String("Added String"));

   assert(row.width() == 4);
   assert(!row.get_bool(0));
   assert(row.get_int(1) == 100000);
   assert(abs(row.get_float(2) - 1717.17) < 0.0001);
   assert(row.get_string(3)->equals(new String("Added String")));
   row_tests += 5;

   assert(row.col_type(0) == 'B');
   assert(row.col_type(1) == 'I');
   assert(row.col_type(2) == 'F');
   assert(row.col_type(3) == 'S');
   row_tests += 4;

   printf("\nRow: All %zu tests passed.\n\n", row_tests);

 }
