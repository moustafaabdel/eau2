#pragma once
//lang::CwC

#include <cstdlib>
#include "../util/object.h"
#include "../util/string.h"

/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 *****************************************************************************/
class Fielder : public Object {
public:

  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  virtual void start(size_t r){}

  /** Called for fields of the argument's type with the value of the field. */
  virtual void accept(bool b){}
  virtual void accept(float f){}
  virtual void accept(int i){}
  virtual void accept(String* s){}

  /** Called when all fields have been seen. */
  virtual void done(){}
};

/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row that prints the current field.
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 *****************************************************************************/
class PrintFielder : public Fielder {
public:

  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  virtual void start(size_t r) override{}

  /** Called for fields of the argument's type with the value of the field. */
  virtual void accept(bool b) override { printf("%d", b); }
  virtual void accept(float f) override { printf("%f", f); }
  virtual void accept(int i) override { printf("%d", i); }
  virtual void accept(String* s) override { printf("%s", s->c_str()); }

  /** Called when all fields have been seen. */
  virtual void done() override{}
};
