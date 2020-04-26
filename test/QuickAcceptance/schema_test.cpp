#include <assert.h>
#include "../../src/dataframe/schema.h"

/***********************************************************
 * Schema Tests.
 **********************************************************/
 int main (int argc, char* argv[]) {

   size_t schema_tests = 0;

   /** Construct an empty schema. */
   Schema empty_schm("");
   assert(empty_schm.width() == 0);
   assert(empty_schm.length() == 0);
   schema_tests += 2;

   /** Add column to empty schema. */
   empty_schm.add_column('I', 3);
   assert(empty_schm.width() == 1);
   assert(empty_schm.length() == 3);
   schema_tests += 2;

   /** Construct a schma using specified types. */
   Schema non_empty_schm("BIFS");
   assert(non_empty_schm.width() == 4);
   assert(non_empty_schm.length() == 0);
   schema_tests += 2;

   non_empty_schm.add_row();
   assert(non_empty_schm.length() == 1);
   schema_tests += 1;

   /** Serialization Tests shown in serial_test.cpp. */


   printf("\nSchema: All %zu tests passed.\n\n", schema_tests);

}
