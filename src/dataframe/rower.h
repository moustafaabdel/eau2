#pragma once
//lang::CwC
#include <stdio.h>
#include "row.h"
#include "fielder.h"
#include "../util/object.h"
#include "../util/list.h"


/*******************************************************************************
 *  Rower::
 *  An interface for iterating through each row of a data frame. The intent
 *  is that this class should subclassed and the accept() method be given
 *  a meaningful implementation. Rowers can be cloned for parallel execution.
 *
 *  Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 ******************************************************************************/
class Rower : public Object {
 public:

    Fielder* f;

  /** This method is called once per row. The row object is on loan and
      should not be retained as it is likely going to be reused in the next
      call. The return value is used in filters to indicate that a row
      should be kept. */
    virtual bool accept(Row& r) { return true; }

  /** Once traversal of the data frame is complete the rowers that were
      split off will be joined.  There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
  virtual void join_delete(Rower* other) { }

  /** Creates a clone of this Rower. */
  virtual Rower* clone() {
    Rower* r = new Rower();
    r->f = f;

    return r;
  }
};


/*******************************************************************************
 *  PrintRower::
 *  Prints all elements in a Row.
 *
 *  Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 ******************************************************************************/
class PrintRower : public Rower {
 public:

    Fielder* f;

  /** This method is called once per row. The row object is on loan and
      should not be retained as it is likely going to be reused in the next
      call. The return value is used in filters to indicate that a row
      should be kept. */
    virtual bool accept(Row& r) {
        PrintFielder* f = new PrintFielder();
        //printf("%s ",r.name->c_str()); //no row way to add row names currently so not needed, API weakness as per Piazza
        for (int i = 0; i < r.width_; i++) {
            if (r.col_types[i] == 'B') {
                f->accept(r.get_bool(i));
            }
            else if (r.col_types[i] == 'I') {
                f->accept(r.get_int(i));
            }
            else if (r.col_types[i] == 'F') {
                f->accept(r.get_float(i));
            }
            else if (r.col_types[i] == 'S') {
                f->accept(r.get_string(i));
            }
          printf(" ");
        }
        printf("\n");

        return true;
    }

  /** Once traversal of the data frame is complete the rowers that were
      split off will be joined.  There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
  virtual void join_delete(Rower* other) {

  }

  /** Creates a clone of this PrintRower. */
  virtual Rower* clone() {
    PrintRower* r = new PrintRower();
    r->f = f;

    return r;
  }
};
