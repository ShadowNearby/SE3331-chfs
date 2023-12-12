#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "map_reduce/protocol.h"

namespace mapReduce {
//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
std::vector<KeyVal> Map(const std::string &content) {
  // Your code goes here
  // Hints: split contents into an array of words.
  std::vector<KeyVal> ret;
  return ret;
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
std::string Reduce(const std::string &key, const std::vector<std::string> &values) {
  // Your code goes here
  // Hints: return the number of occurrences of the word.
  std::string ret = "0";
  return ret;
}
}  // namespace mapReduce