#pragma once
//lang::CwC

#include <assert.h>
#include "object.h"
#include "string.h"

/****************************************************************************
 * StrList::
 *
 * Represents a list of String* values.
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 ***************************************************************************/
class StrList : public Object {
public:
	String** values;
	size_t length_;

	StrList() : Object() {
		values = nullptr;
		length_ = 0;
	}

	StrList(String** str, size_t size) : Object() {
		values = new String*[size];

		for(size_t i = 0; i < size; i++) {
			values[i] = str[i];
		}

		length_ = size;
	}

	virtual ~StrList() override {
	    delete[] values;
	}

	/** Adds specified String* value at the end of values. */
	void push_back(String* e) {
		add(length_, e);
	}

	/** Adds specified String* value to values at the specified index. */
	void add(size_t i, String* e) {
		assert(i <= length_);

		String** newArr = nullptr;
		size_t newSize = length_ + 1;
		newArr = new String*[newSize];

		for (size_t j = 0; j < newSize; ++j)
		{
			if (j < i) {
				newArr[j] = values[j];
			}
			else if (j > i) {
				newArr[j] = values[j - 1];
			}
			else {
				newArr[j] = e;
			}
		}

		delete[] values;
		values = newArr;
		length_ = newSize;
	}

	/** Inserts all of elements in c into this list at specified index. */
	void add_all(size_t i, StrList* c) {
		size_t cSize = c->size();

		for (int j = 0; j < cSize; ++j)
		{
			add(j + i, c->values[j]);
		}
	}

	/** Removes all of elements from this list. */
	void clear() {
		delete[] values;
		values = nullptr;
		length_ = 0;
	}

	/** Compares o with this list for equality. */
	bool equals(Object* o) override {
		StrList* strlist = dynamic_cast<StrList*>(o);

    	if(strlist == nullptr || strlist->length_ != length_) {
    		return false;
    	}

    	for (int i = 0; i < length_; ++i)
    	{
    		if(values[i]->equals(strlist->values[i]) == false) {
    			return false;
    		}
    	}

    	return true;
	}

	/** Returns the element at index. */
	String* get(size_t index) {
		assert(index < length_);

		return values[index];
	}

	/** Returns the hash code value for this list. */
	size_t hash() {
		size_t hash = length_;

    	for (size_t i = 0; i < length_; ++i) {
    		hash += i * values[i]->hash();
    	}

    	return hash;
	}

	/** Returns the index of the first occurrence of o, or >size() if not there. */
	size_t index_of(Object* o) {
		for (size_t i = 0; i < length_; ++i)
		{
			if (values[i]->equals(o))
			{
				return i;
			}
		}

		return length_ + 1;
	}

	/** Return the number of elements in the collection. */
	size_t size() {
		return length_;
	}

};

/****************************************************************************
 * SortedStrList::
 *
 * Represents a list of strings that maintains its elements ordered
 * by its method of comparison
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 ***************************************************************************/
class SortedStrList : public StrList {
public:

	SortedStrList() : StrList(){

	}
	//SortedStrList(String* str);
	//SortedStrList(StrList list);
	virtual ~SortedStrList() override {

	}
	/** adds the given element to the list in proper sorted location. */
	void sorted_add(String* e) {
		size_t indexToAdd = length_;
		for (size_t i = 0; i < length_; ++i)
		{
			if(e->compare(values[i]) < 0 && i < indexToAdd) {
				indexToAdd = i;
			}
		}

		StrList::add(indexToAdd, e);
	}

	/** Compares o with this list for equality. */
	bool equals(Object* o) override {
		return StrList::equals(o);
	}

	/** Returns the hash code value for this list. */
	size_t hash()  {
		return StrList::hash() + 1;
	}
};

/****************************************************************************
 * SortedStrList::
 *
 * Represents a list of floats that maintains its elements ordered
 * by its method of comparison
 *
 * Authors: abdelaziz.m@husky.neu.edu & shaikh.mo@husky.neu.edu
 ***************************************************************************/
class SortedNumList {
public:
	float* values;
	size_t length_;

	SortedNumList() {
		values = nullptr;
		length_ = 0;
	}

	virtual ~SortedNumList() {
		delete[] values;
	}

	/** Adds the specified float into the sorted list of values. */
	void sorted_add(float f) {
		size_t indexToAdd = length_;

		for (size_t i = 0; i < length_; i++) {
			if (f < values[i] && i < indexToAdd) {
				indexToAdd = i;
			}
		}
		add(indexToAdd, f);
	}

	/** Adds the specified float into the list of values at a specified index. */
	void add(size_t i, float f) {
		assert(i <= length_);

		float* newArr = nullptr;
		size_t newSize = length_ + 1;
		newArr = new float[newSize];

		for (size_t j = 0; j < newSize; ++j)
		{
			if (j < i) {
				newArr[j] = values[j];
			}
			else if (j > i) {
				newArr[j] = values[j - 1];
			}
			else {
				newArr[j] = f;
			}
		}

		delete[] values;
		values = newArr;
		length_ = newSize;
	}

	/** Return the number of elements in the collection. */
	size_t size() {
		return length_;
	}
};
